import ast
import pathlib

from models.base import MediaType
from models.media import Media, _stamp_media_uri
from models.show import Show, _stamp_show_uri

ROUTERS = pathlib.Path(__file__).resolve().parent.parent / "routers"


def _chunked_select_calls(tree: ast.AST):
    """Yield (helper_name, select_call) for every _select*_in_chunks(db, lambda: select(...))."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id not in ("_select_in_chunks", "_select_rows_in_chunks"):
            continue
        for arg in node.args:
            if not isinstance(arg, ast.Lambda):
                continue
            body = arg.body
            if isinstance(body, ast.Call) and isinstance(body.func, ast.Attribute):
                body = body.func.value
                while isinstance(body, ast.Call) and isinstance(body.func, ast.Attribute):
                    body = body.func.value
            if isinstance(body, ast.Call) and isinstance(body.func, ast.Name) and body.func.id == "select":
                yield node.func.id, body


def test_scalar_chunk_helper_is_never_given_a_multi_column_select():
    """_select_in_chunks collapses rows with .scalars(), so a multi-column select
    silently loses every column after the first."""
    offenders = []
    for path in ROUTERS.glob("*.py"):
        tree = ast.parse(path.read_text())
        for helper, select_call in _chunked_select_calls(tree):
            columns = len(select_call.args)
            if helper == "_select_in_chunks" and columns > 1:
                offenders.append(f"{path.name}:{select_call.lineno} selects {columns} columns")
            if helper == "_select_rows_in_chunks" and columns == 1:
                offenders.append(f"{path.name}:{select_call.lineno} selects 1 column")
    assert offenders == [], offenders


def test_media_uri_is_stamped_from_tmdb_id():
    for media_type, expected in [
        (MediaType.movie, "tmdb:m:550"),
        (MediaType.series, "tmdb:s:550"),
        (MediaType.episode, "tmdb:e:550"),
    ]:
        media = Media(tmdb_id=550, media_type=media_type, title="t")
        _stamp_media_uri(None, None, media)
        assert media.uri_id == expected


def test_media_uri_is_left_alone_when_already_set_or_unidentified():
    media = Media(tmdb_id=550, media_type=MediaType.movie, title="t", uri_id="imdb:m:137523")
    _stamp_media_uri(None, None, media)
    assert media.uri_id == "imdb:m:137523"

    stub = Media(tmdb_id=None, media_type=MediaType.episode, title="t")
    _stamp_media_uri(None, None, stub)
    assert stub.uri_id is None


def test_show_uri_prefers_tmdb_then_falls_back_to_tvdb():
    show = Show(tmdb_id=1396, tvdb_id=81189, title="t")
    _stamp_show_uri(None, None, show)
    assert show.uri_id == "tmdb:s:1396"

    tvdb_only = Show(tvdb_id=81189, title="t")
    _stamp_show_uri(None, None, tvdb_only)
    assert tvdb_only.uri_id == "tvdb:s:81189"
