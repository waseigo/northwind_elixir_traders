# SPDX-FileCopyrightText: 2025 Isaak Tsalicoglou <isaak@overbring.com>
# SPDX-License-Identifier: Apache-2.0

defmodule NorthwindElixirTraders.Validations do
  import Ecto.Changeset
  alias NorthwindElixirTraders.{Repo, Country}

  @age_mn 15
  @age_mx 100

  def validate_foreign_key_id(changeset, target, field) when is_atom(field) do
    val = get_field(changeset, field)

    if is_nil(val) do
      add_error(changeset, field, "key '%{field}' not found in changeset",
        field: field,
        validation: :foreign_key_id
      )
    else
      target_record = Repo.get(target, val)

      case target_record do
        nil ->
          add_error(changeset, field, "no '%{record}' with primary key value '%{pkval}'",
            record: to_string(target),
            pkval: val,
            validation: :foreign_key_id
          )

        _ ->
          changeset
      end
    end
  end

  def validate_country(changeset, field) when is_atom(field) do
    case {changeset.errors[field], get_field(changeset, field)} do
      {nil, country} when not is_nil(country) ->
        if is_nil(Country.get_dial(country)) do
          add_error(
            changeset,
            field,
            "country '%{country}' not found in the Countries table",
            country: country,
            validation: :country
          )
        else
          changeset
        end

      _ ->
        changeset
    end
  end

  def validate_age_range(
        changeset,
        field,
        [min: age_mn, max: age_mx] = opts \\ [min: @age_mn, max: @age_mx]
      )
      when is_atom(field) and is_list(opts) do
    case {changeset.errors[field], get_field(changeset, field)} do
      {nil, field_value} when not is_nil(field_value) ->
        within_range? =
          field_value
          |> Date.diff(Date.utc_today())
          |> then(&Kernel.and(Kernel.>(&1, -age_mx * 365), Kernel.<(&1, -age_mn * 365)))

        if not within_range? do
          add_error(
            changeset,
            field,
            "date not within the specified span between '%{min}' and '%{max}' years ago",
            min: age_mn,
            max: age_mx,
            validation: :age_range
          )
        else
          changeset
        end

      _ ->
        changeset
    end
  end
end
