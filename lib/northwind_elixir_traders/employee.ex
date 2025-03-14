# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Employee do
  use Ecto.Schema
  import Ecto.Changeset
  alias NorthwindElixirTraders.{Order, Validations}

  @name_mxlen 50
  @notes_mxlen 500

  schema "employees" do
    field(:last_name, :string)
    field(:first_name, :string)
    field(:name, :string, virtual: true)
    field(:birth_date, :date)
    field(:photo, :string)
    field(:notes, :string)
    has_many(:orders, Order, on_replace: :nilify)

    timestamps(type: :utc_datetime)
  end

  def import_changeset(data, params \\ %{}) do
    permitted = [:id, :last_name, :first_name, :birth_date, :photo, :notes]
    required = permitted

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_length(:last_name, max: @name_mxlen)
    |> validate_length(:first_name, max: @name_mxlen)
    |> validate_length(:notes, max: @notes_mxlen)
    |> Validations.validate_age_range(:birth_date, min: 15, max: 100)
    |> unique_constraint([:last_name, :first_name, :birth_date])
    |> unique_constraint([:id])
  end

  def populate_name(%__MODULE__{first_name: first, last_name: last} = e) do
    %{e | name: last <> " " <> first}
  end
end
