defmodule NorthwindElixirTraders.PhoneNumbers do
  @intl_regex ~r/^\+((?:9[679]|8[035789]|6[789]|5[90]|42|3[578]|2[1-689])|9[0-58]|8[1246]|6[0-6]|5[1-8]|4[013-9]|3[0-469]|2[70]|7|1)(?:\W*\d){0,13}\d$/
  @nanp_regex ~r/^\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})$/

  def is_intl?(phone) when is_bitstring(phone), do: Regex.match?(@intl_regex, phone)

  def is_nanp?(phone) when is_bitstring(phone), do: Regex.match?(@nanp_regex, phone)

  def make_readable(phone) when is_bitstring(phone) do
    phone
    |> then(&Regex.replace(~r/[^\d\s]/, &1, " "))
    |> then(&Regex.replace(~r/\s+/, &1, " "))
    |> String.trim_leading()
  end

  def compose_intl(dialcode, readable)
      when is_bitstring(dialcode) and is_bitstring(readable) do
    to_string(["+", dialcode, " ", readable])
  end
end
