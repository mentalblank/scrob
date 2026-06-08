import secrets
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from core import plex
from core.config import settings as app_settings
from core.security import create_access_token
from db import get_db
from dependencies import get_current_user
from models.connections import MediaServerConnection
from models.users import User
from routers.auth import _registration_allowed, _generate_api_key

import logging

logger = logging.getLogger(__name__)

router = APIRouter()


class PinRequest(BaseModel):
    pin_id: str


class CreateConnectionsRequest(BaseModel):
    pin_id: str
    client_identifiers: list[str]


def _forward_url(link: bool) -> str:
    page = "/plex-link-callback" if link else "/plex-callback"
    return f"{app_settings.server_url.rstrip('/')}{page}"


@router.get("/config")
async def plex_config():
    return {"enabled": True}


@router.get("/authorize")
async def plex_authorize(link: bool = False, redirect_uri: Optional[str] = None):
    """Create a Plex auth PIN and return the app.plex.tv URL to redirect to.

    redirect_uri is supplied by the frontend so it works behind a reverse proxy.
    """
    forward = redirect_uri if (redirect_uri or "").startswith(("http://", "https://")) else _forward_url(link)
    pin = await plex.create_auth_pin()
    if not pin:
        raise HTTPException(status_code=502, detail="Could not reach Plex to start sign-in")
    auth_url = plex.build_auth_url(pin["code"], forward)
    return {"auth_url": auth_url, "pin_id": str(pin["id"])}


async def _resolve_account(pin_id: str) -> dict:
    """Exchange a claimed PIN for the Plex account (+ its authToken), or raise."""
    auth_token = await plex.check_auth_pin(pin_id)
    if not auth_token:
        raise HTTPException(status_code=400, detail="Plex sign-in was not completed")
    account = await plex.get_account(auth_token)
    if not account:
        raise HTTPException(status_code=400, detail="Could not load your Plex account")
    account["auth_token"] = auth_token
    return account


async def _existing_plex_urls(db: AsyncSession, user_id: int) -> set:
    result = await db.execute(
        select(MediaServerConnection.url).where(
            MediaServerConnection.user_id == user_id,
            MediaServerConnection.type == "plex",
        )
    )
    return {row[0] for row in result.all()}


async def _discover_servers(db: AsyncSession, user_id: int, auth_token: str) -> list[dict]:
    """List the account's reachable Plex servers for the user to pick from (no tokens)."""
    try:
        servers = await plex.get_servers(auth_token)
    except Exception as exc:
        logger.warning("Plex server discovery failed for user %s: %s", user_id, exc)
        return []
    existing = await _existing_plex_urls(db, user_id)
    return [
        {
            "name": srv["name"],
            "client_identifier": srv["client_identifier"],
            "url": srv["url"],
            "owned": srv["owned"],
            "already_added": srv["url"] in existing,
        }
        for srv in servers
    ]


@router.post("/exchange")
async def plex_exchange(body: PinRequest, db: AsyncSession = Depends(get_db)):
    """Log in or create a user from a completed Plex PIN. Returns a JWT."""
    account = await _resolve_account(body.pin_id)

    # 1. Match by stored Plex account id.
    result = await db.execute(select(User).where(User.plex_account_id == account["id"]))
    user = result.scalar_one_or_none()

    # 2. Fall back to email; link Plex to that existing account.
    if not user and account["email"]:
        result = await db.execute(select(User).where(User.email == account["email"]))
        user = result.scalar_one_or_none()
        if user:
            user.plex_account_id = account["id"]
            user.plex_username = account["username"]

    # 3. Auto-create (gated by registration policy).
    if not user:
        if not account["email"]:
            raise HTTPException(status_code=400, detail="Your Plex account has no email address")
        if not await _registration_allowed(db):
            raise HTTPException(status_code=403, detail="No account found and registrations are disabled")

        username = (account["username"] or account["email"].split("@")[0])[:100]
        base, counter = username, 1
        while True:
            exists = await db.execute(select(User).where(User.username == username))
            if not exists.scalar_one_or_none():
                break
            username = f"{base}{counter}"
            counter += 1

        user = User(
            email=account["email"],
            username=username,
            password_hash=None,
            api_key=_generate_api_key(),
            email_confirmed=True,
            plex_account_id=account["id"],
            plex_username=account["username"],
        )
        db.add(user)

    await db.commit()
    await db.refresh(user)
    return {"access_token": create_access_token(subject=user.id)}


@router.post("/link")
async def plex_link(
    body: PinRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Link the authenticated account to a Plex account."""
    account = await _resolve_account(body.pin_id)

    existing = await db.execute(
        select(User).where(User.plex_account_id == account["id"], User.id != current_user.id)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="That Plex account is already linked to another user")

    current_user.plex_account_id = account["id"]
    current_user.plex_username = account["username"]
    await db.commit()

    # Offer the account's servers for the user to pick from (nothing added yet).
    servers = await _discover_servers(db, current_user.id, account["auth_token"])
    return {"status": "linked", "plex_username": account["username"], "servers": servers}


@router.post("/connections")
async def plex_create_connections(
    body: CreateConnectionsRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create media-server connections for the Plex servers the user selected."""
    account = await _resolve_account(body.pin_id)
    if current_user.plex_account_id and current_user.plex_account_id != account["id"]:
        raise HTTPException(status_code=403, detail="That Plex PIN belongs to a different account")

    chosen = set(body.client_identifiers)
    if not chosen:
        return {"added": 0}

    servers = await plex.get_servers(account["auth_token"])
    existing = await _existing_plex_urls(db, current_user.id)

    added = 0
    for srv in servers:
        if srv["client_identifier"] not in chosen or srv["url"] in existing:
            continue
        db.add(MediaServerConnection(
            user_id=current_user.id,
            type="plex",
            name=srv["name"],
            url=srv["url"],
            token=srv["token"],
            server_username=current_user.plex_username or None,
        ))
        existing.add(srv["url"])
        added += 1

    await db.commit()
    return {"added": added}


@router.post("/unlink")
async def plex_unlink(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Unlink Plex from the account. Refused if it would leave no way to sign in."""
    if not current_user.password_hash:
        raise HTTPException(
            status_code=400,
            detail="Set a password before unlinking Plex, or you'll be locked out.",
        )
    current_user.plex_account_id = None
    current_user.plex_username = None
    await db.commit()
    return {"status": "unlinked"}
