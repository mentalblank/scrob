/// <reference path="../.astro/types.d.ts" />
/// <reference types="astro/client" />

declare namespace App {
  interface Locals {
    user: {
      id: number;
      username: string;
      display_name: string;
      email: string;
      role: string;
      is_admin?: boolean;
      avatar_url?: string | null;
    } | null;
    token: string | undefined;
  }
}
