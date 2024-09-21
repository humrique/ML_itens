def return_item_value_text_strip(value, default_return):
    return value.text.strip() if value else default_return
