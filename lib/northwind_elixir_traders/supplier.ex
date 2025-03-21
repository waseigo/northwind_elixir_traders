# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Supplier do
  use Ecto.Schema
  import Ecto.Changeset
  alias NorthwindElixirTraders.{Product, PhoneNumbers, Validations}

  @name_mxlen 50

  schema "suppliers" do
    field(:name, :string)
    field(:contact_name, :string)
    field(:address, :string)
    field(:city, :string)
    field(:postal_code, :string)
    field(:country, :string)
    field(:phone, :string)
    has_many(:products, Product, on_replace: :nilify)
    has_many(:orders, through: [:products, :order_details, :order])

    timestamps(type: :utc_datetime)
  end

  def import_changeset(data, params \\ %{}) do
    permitted = [:id, :name, :contact_name, :address, :city, :postal_code, :country, :phone]
    required = permitted

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_length(:name, max: @name_mxlen)
    |> Validations.validate_country(:country)
    |> PhoneNumbers.validate_phone(:phone, :country)
    |> unique_constraint([:name])
    |> unique_constraint([:id])
  end
end
