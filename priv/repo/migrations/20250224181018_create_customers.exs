# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Repo.Migrations.CreateCustomers do
  use Ecto.Migration

  def change do
    create table(:customers) do
      add :name, :string, null: false
      add :contact_name, :string
      add :address, :string
      add :city, :string
      add :postal_code, :string
      add :country, :string

      timestamps(type: :utc_datetime)
    end

    create unique_index(:customers, [:name])
  end
end
