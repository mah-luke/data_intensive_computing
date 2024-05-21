def merge_dicts(dict1: dict, dict2: dict):
    for category, doc_cnt in dict2.items():
        if category not in dict1:
            dict1[category] = doc_cnt
        else:
            dict1[category] += doc_cnt
    return dict1


def calculate_chi_squares(a, b, c, d, n):
    return (n * ((a * d - b * c) ** 2)) / ((a + b) * (a + c) * (b + d) * (c + d))


def printable_category(category: str, top75: list[tuple[str, int]]):
    return f"<{category}> {' '.join([f'{tup[0]}:{tup[1]}' for tup in top75])}"

def calculate_chi_square_per_token(cur_category_counts: tuple[str, dict[str, int]], category_counts, reviews_cnt):
    doc_cnt_term = sum(cur_category_counts[1].values())

    for category, doc_cnt_cur_term in cur_category_counts[1].items():
        a = doc_cnt_cur_term
        b = doc_cnt_term - a
        c = category_counts[category] - a
        d = reviews_cnt - a - b - c
        yield category, (cur_category_counts[0], calculate_chi_squares(a,b,c,d,reviews_cnt))
