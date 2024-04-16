def calculate_chi_squares(a, b, c, d, n):
    return (n * ((a * d - b * c) ** 2)) / ((a + b) * (a + c) * (b + d) * (c + d))
