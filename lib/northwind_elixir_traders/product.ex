# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Product do
  use Ecto.Schema
  import Ecto.Changeset
  alias NorthwindElixirTraders.{Category, Validations, Supplier, OrderDetail}

  @name_mxlen 50

  schema "products" do
    field(:name, :string)
    field(:unit, :string)
    field(:price, :integer)
    field(:category_id, :integer)
    belongs_to(:category, Category, define_field: false)
    belongs_to(:supplier, Supplier)
    has_many(:order_details, OrderDetail)
    has_many(:orders, through: [:order_details, :order])

    timestamps(type: :utc_datetime)
  end

  def import_changeset(data, params \\ %{}) do
    permitted = [:id, :name, :unit, :price, :category_id, :supplier_id]
    required = permitted

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_length(:name, max: @name_mxlen)
    |> Validations.validate_foreign_key_id(Category, :category_id)
    |> Validations.validate_foreign_key_id(Supplier, :supplier_id)
    |> validate_number(:price, greater_than_or_equal_to: 1)
    |> unique_constraint([:name])
    |> unique_constraint([:id])
  end
end
