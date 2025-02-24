defmodule NorthwindElixirTraders.PhoneNumbers do
  @intl_regex ~r/^\+((?:9[679]|8[035789]|6[789]|5[90]|42|3[578]|2[1-689])|9[0-58]|8[1246]|6[0-6]|5[1-8]|4[013-9]|3[0-469]|2[70]|7|1)(?:\W*\d){0,13}\d$/

  def is_intl?(phone) when is_bitstring(phone), do: Regex.match?(@intl_regex, phone)
end
