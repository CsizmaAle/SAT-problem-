import random

# Configuration
num_formulas = 10    # Number of CNF formulas to generate.
num_clauses = 20     # Number of clauses in each formula.
num_literals = 10    # Variables are in the range [1, num_literals].
unsat_prob = 0.3     # Probability to inject a pair of contradictory clauses.

def generate_formula(num_clauses, num_literals, unsat_prob):
    """
    Generates a single CNF formula (list of clauses).

    Parameters:
      num_clauses: Total number of clauses for the formula.
      num_literals: Variables are chosen from 1 to num_literals.
      unsat_prob: With this probability, a pair of contradictory unit clauses is injected.

    Returns:
      A list of clauses, where each clause is a list of integers.
    """
    formula = []
    
    # Decide whether to inject a contradictory pair.
    inject_unsat = random.random() < unsat_prob
    
    if inject_unsat and num_clauses >= 2:
        # Choose one literal randomly.
        chosen_literal = random.choice(range(1, num_literals + 1))
        # Inject contradictory unit clauses: one [l] and one [-l].
        formula.append([chosen_literal])
        formula.append([-chosen_literal])
        clauses_remaining = num_clauses - 2
    else:
        clauses_remaining = num_clauses

    # Generate the remaining clauses.
    # Here each clause will have between 1 and min(3, num_literals) literals.
    for _ in range(clauses_remaining):
        clause_size = random.randint(1, min(3, num_literals))
        # Randomly select distinct variables.
        vars_chosen = random.sample(range(1, num_literals + 1), clause_size)
        clause = []
        for v in vars_chosen:
            # Randomly decide the sign of the literal.
            literal = v if random.choice([True, False]) else -v
            clause.append(literal)
        formula.append(clause)
    
    return formula

def write_input_file(filename, formulas):
    """
    Writes a list of formulas to a file.
    Each clause is on a separate line; an empty line is added between formulas.
    """
    with open(filename, "w") as f:
        for formula in formulas:
            for clause in formula:
                line = " ".join(map(str, clause))
                f.write(line + "\n")
            f.write("\n")  # empty line separating formulas

if __name__ == "__main__":
    # Generate the formulas.
    formulas = [generate_formula(num_clauses, num_literals, unsat_prob)
                for _ in range(num_formulas)]
    
    # Write the formulas to the file.
    write_input_file("input.txt", formulas)
    print(f"input.txt generated with {num_formulas} formulas.")
