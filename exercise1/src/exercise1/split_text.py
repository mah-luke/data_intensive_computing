import re

def split_text(text):
    pattern_split_characters = re.compile("[\s\d\(\)\[\]{}\.!\?,;:\+=\-_\"'`~#@&\*%€\$§\\\/]")
    text = pattern_split_characters.sub(" ", text)
    text = text.lower()
    pattern_single_character = re.compile("(^|\s)\w(\s|$)")
    text = pattern_single_character.sub(" ", text)
    return text.split()