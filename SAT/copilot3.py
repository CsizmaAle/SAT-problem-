import time
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

# Increase recursion limit for deep recursions in DP and DPLL
sys.setrecursionlimit(10000)


# ---------------------------
# STREAMING INPUT PARSER (GENERATOR)
# ---------------------------
def parse_input_file_generator(filename):
    """
    Generator version: yields one set of clauses at a time.
    Each set is a list of clauses; each clause is a list of integers.
    Clauses are provided on separate lines, and an empty line signals a new set.
    """
    current_set = []
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            if line == "":
                if current_set:
                    yield current_set
                    current_set = []
            else:
                # Each clause is represented by space-separated integers
                clause = list(map(int, line.split()))
                current_set.append(clause)
        if current_set:
            yield current_set


# ---------------------------
# OUTPUT WRITER
# ---------------------------
def write_output_file(filename, results):
    """
    Writes the results into an output file.
    'results' is a list where each element is a dictionary mapping algorithm names
    to a tuple (result boolean, execution time).
    """
    with open(filename, "w") as f:
        for i, res in enumerate(results):
            f.write(f"Set {i+1}:\n")
            for alg_name, (result, duration) in res.items():
                res_text = "Satisfiable" if result else "Unsatisfiable"
                f.write(f"  {alg_name}: {res_text}, {duration:.6f} sec\n")
            f.write("\n")


# ---------------------------
# ALGORITHMS
# ---------------------------

# --- Resolution Algorithm ---
def resolve_pair(ci, cj):
    """
    For two clauses (as frozensets), try to resolve on any pair of complementary literals.
    Return a set of possible resolvents.
    """
    resolvents = set()
    for literal in ci:
        if -literal in cj:
            resolvent = (ci - {literal}) | (cj - {-literal})
            resolvents.add(frozenset(resolvent))
    return resolvents

def resolution(clauses):
    """
    Resolution algorithm:
      - Clauses are converted into frozensets to ensure immutability and easy duplicate checks.
      - Iteratively applies the resolution rule until either:
          • the empty clause is derived (unsatisfiable), or
          • no new clauses can be added.
    Returns:
      - False if unsatisfiability is proven (empty clause derived).
      - True if no refutation is found.
    """
    clauses_set = set(frozenset(clause) for clause in clauses)
    new = set()
    while True:
        # Evaluate all distinct clause pairs
        pairs = [(ci, cj) for ci in clauses_set for cj in clauses_set if ci != cj]
        for (ci, cj) in pairs:
            resolvents = resolve_pair(ci, cj)
            if frozenset() in resolvents:  # derived empty clause
                return False
            new = new.union(resolvents)
        if new.issubset(clauses_set):
            return True  # no new clause can be added
        clauses_set.update(new)


# --- Davis-Putnam (DP) Algorithm ---
def dp(clauses):
    """
    Simple Davis-Putnam recursive algorithm.
    Base cases:
      - If an empty clause is found: unsatisfiable.
      - If there are no clauses left: satisfiable.
    Then, a variable is selected and eliminated using resolution-like steps.
    """
    if any(len(clause) == 0 for clause in clauses):
        return False
    if not clauses:
        return True

    vars_set = {abs(lit) for clause in clauses for lit in clause}
    if not vars_set:
        return True

    p = next(iter(vars_set))
    pos_clauses = [clause for clause in clauses if p in clause]
    neg_clauses = [clause for clause in clauses if -p in clause]
    new_clauses = [clause for clause in clauses if p not in clause and -p not in clause]

    resolvents = []
    for ci in pos_clauses:
        for cj in neg_clauses:
            resolvent = (set(ci) - {p}) | (set(cj) - {-p})
            # Skip tautologies
            if any(x in resolvent and -x in resolvent for x in resolvent):
                continue
            resolvents.append(list(resolvent))
    new_clauses.extend(resolvents)
    return dp(new_clauses)


# --- DPLL Algorithm ---
def simplify_clauses(clauses, assignment):
    """
    Simplify clauses given a partial assignment:
      - Remove literals that are false in each clause.
      - Remove satisfied clauses.
    Clauses are represented as sets.
    """
    new_clauses = []
    for clause in clauses:
        new_clause = set()
        satisfied = False
        for lit in clause:
            var = abs(lit)
            if var in assignment:
                if (lit > 0 and assignment[var]) or (lit < 0 and not assignment[var]):
                    satisfied = True
                    break
            else:
                new_clause.add(lit)
        if not satisfied:
            new_clauses.append(new_clause)
    return new_clauses

def dpll(clauses, assignment):
    """
    Recursively implements the DPLL algorithm:
      - Uses unit propagation and pure literal elimination.
      - Makes a decision on a literal's truth value when needed.
    Returns True if the formula is satisfiable, otherwise False.
    """
    clauses = simplify_clauses(clauses, assignment)
    if any(len(clause) == 0 for clause in clauses):
        return False
    if not clauses:
        return True

    unit_clauses = [clause for clause in clauses if len(clause) == 1]
    if unit_clauses:
        for clause in unit_clauses:
            lit = next(iter(clause))
            assignment[abs(lit)] = (lit > 0)
        return dpll(clauses, assignment)

    # Check for pure literal elimination.
    all_lits = {lit for clause in clauses for lit in clause}
    for lit in list(all_lits):
        if -lit not in all_lits:
            assignment[abs(lit)] = (lit > 0)
            return dpll(clauses, assignment)

    # Choose the next literal from the first clause that has not been assigned.
    for clause in clauses:
        for lit in clause:
            if abs(lit) not in assignment:
                # Try a truth assignment for the chosen literal.
                assignment_copy = assignment.copy()
                assignment_copy[abs(lit)] = (lit > 0)
                if dpll(clauses, assignment_copy):
                    assignment.update(assignment_copy)
                    return True
                # Try the opposite assignment.
                assignment_copy = assignment.copy()
                assignment_copy[abs(lit)] = not (lit > 0)
                if dpll(clauses, assignment_copy):
                    assignment.update(assignment_copy)
                    return True
                return False
    return False

def dpll_wrapper(clauses):
    """
    Wrapper for the DPLL algorithm:
      - Converts clause lists into sets.
      - Returns a tuple (result, assignment).
    """
    clauses_sets = [set(clause) for clause in clauses]
    assignment = {}
    result = dpll(clauses_sets, assignment)
    return result, assignment


# ---------------------------
# PARALLEL PROCESSING OF CLAUSE SETS
# ---------------------------
def process_clause_set(clauses, set_index):
    """
    Process a single set of clauses using Resolution, DP, and DPLL.
    Returns a tuple (set_index, results_dict).
    """
    results = {}

    start = time.time()
    res_resolution = resolution(clauses)
    results["Resolution"] = (res_resolution, time.time() - start)

    start = time.time()
    res_dp = dp(clauses)
    results["DP"] = (res_dp, time.time() - start)

    start = time.time()
    res_dpll, assignment = dpll_wrapper(clauses)
    results["DPLL"] = (res_dpll, time.time() - start)

    return set_index, results


# ---------------------------
# MAIN FUNCTION USING PARALLEL PROCESSING
# ---------------------------
def main_parallel():
    input_filename = "input.txt"
    output_filename = "output.txt"

    # Read each set of clauses using the generator (streaming)
    clause_sets = list(parse_input_file_generator(input_filename))
    results = {}

    # Use a process pool to process each clause set concurrently
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_clause_set, clauses, idx): idx
                   for idx, clauses in enumerate(clause_sets)}
        for future in as_completed(futures):
            set_index, result = future.result()
            results[set_index] = result

    # Sort results by index
    sorted_results = [results[i] for i in sorted(results.keys())]
    write_output_file(output_filename, sorted_results)
    print(f"Processed {len(clause_sets)} clause sets. Results written to '{output_filename}'.")


if __name__ == "__main__":
    main_parallel()
