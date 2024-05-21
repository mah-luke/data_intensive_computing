import re

pattern_split_characters: re.Pattern = re.compile("[\s\d\(\)\[\]{}\.!\?,;:\+=\-_\"'`~#@&\*%€\$§\\\/]")
pattern_single_character: re.Pattern = re.compile("(^|\s)\w(\s|$)")


def split_text(text,
               pattern_split_characters=pattern_split_characters,
               pattern_single_character=pattern_single_character):
    """Split a string according to the specifications.
    First pattern specifies characters to split on.
    Second pattern matches words with one character."""
    text = pattern_split_characters.sub(" ", text)
    text = text.lower()
    text = pattern_single_character.sub(" ", text)
    return text.split()



