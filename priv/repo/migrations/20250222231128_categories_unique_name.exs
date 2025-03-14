# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Repo.Migrations.CategoriesUniqueName do
  use Ecto.Migration

  def change, do: create unique_index(:categories, [:name])
end
