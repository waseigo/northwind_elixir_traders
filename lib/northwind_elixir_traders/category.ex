# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Category do
  use Ecto.Schema
  import Ecto.Changeset
  alias NorthwindElixirTraders.Product

  @name_mxlen 50
  @desc_mxlen 100

  schema "categories" do
    field(:name, :string)
    field(:description, :string)
    has_many(:products, Product, on_replace: :nilify)
    has_many(:orders, through: [:products, :order_details, :order])

    timestamps(type: :utc_datetime)
  end

  def import_changeset(data, params \\ %{}) do
    permitted = [:id, :name, :description]
    required = permitted

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_length(:name, max: @name_mxlen)
    |> validate_length(:description, max: @desc_mxlen)
    |> unique_constraint([:name])
    |> unique_constraint([:id])
  end
end
