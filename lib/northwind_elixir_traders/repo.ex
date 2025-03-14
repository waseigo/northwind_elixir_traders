# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Repo do
  use Ecto.Repo,
    otp_app: :northwind_elixir_traders,
    adapter: Ecto.Adapters.SQLite3
end
