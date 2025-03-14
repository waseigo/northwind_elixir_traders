# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Repo.Migrations.AddQueryOptimizationIndexes do
  use Ecto.Migration

  def change do
    create index(:orders, [:date])
    create index(:order_details, [:order_id])
    create index(:order_details, [:product_id])
    create index(:products, [:category_id])
    create index(:products, [:supplier_id])
    create index(:orders, [:employee_id])
    create index(:orders, [:customer_id])
    create index(:orders, [:shipper_id])
  end
end
