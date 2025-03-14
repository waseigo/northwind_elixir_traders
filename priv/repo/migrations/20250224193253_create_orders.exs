# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Repo.Migrations.CreateOrders do
  use Ecto.Migration

  def change do
    create table(:orders) do
      add :customer_id, references(:customers)
      add :employee_id, references(:employees)
      add :shipper_id, references(:shippers)
      add :date, :utc_datetime

      timestamps(type: :utc_datetime)
    end
  end
end
