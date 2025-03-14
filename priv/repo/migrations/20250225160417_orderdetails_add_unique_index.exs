# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Repo.Migrations.OrderdetailsAddUniqueIndex do
  use Ecto.Migration

  def change do
    create unique_index(:order_details, [:product_id, :order_id])
  end
end
