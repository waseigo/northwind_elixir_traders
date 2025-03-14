# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.OrderDetail do
  use Ecto.Schema
  import Ecto.Changeset
  alias NorthwindElixirTraders.{Product, Order, Validations}

  schema "order_details" do
    field(:quantity, :integer)
    belongs_to(:order, Order)
    belongs_to(:product, Product)

    timestamps(type: :utc_datetime)
  end

  def import_changeset(data, params \\ %{}) do
    permitted = [:id, :quantity, :order_id, :product_id]
    required = permitted

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_number(:quantity, greater_than: 0)
    |> Validations.validate_foreign_key_id(Order, :order_id)
    |> Validations.validate_foreign_key_id(Product, :product_id)
    |> unique_constraint([:product_id, :order_id])
    |> unique_constraint([:id])
  end
end
