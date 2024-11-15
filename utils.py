def clean_text(text):
    """
    Removes empty lines, lines with only whitespace, and trims whitespace
    from the beginning and end of each line.
    """
    cleaned_lines = []
    for line in text.splitlines():
        stripped_line = line.strip()  # Remove leading and trailing whitespace
        if stripped_line:  # Only add non-empty lines
            cleaned_lines.append(stripped_line)
    return "\n".join(cleaned_lines)

