# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Repo.Migrations.ProductBelongsToCategory do
  use Ecto.Migration

  def change do
    alter table(:products) do
      add :category_id, references(:categories)
    end
  end
end
