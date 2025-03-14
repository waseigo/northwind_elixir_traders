# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Repo.Migrations.ProductBelongsToSupplier do
  use Ecto.Migration

  def change do
    alter table(:products) do
      add :supplier_id, references(:suppliers)
    end
  end
end
