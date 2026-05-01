
import re

with open("dags/job_search_dag.py", "r") as f:
    content = f.read()

old = "        # Overall score\n        overall = matcher.compute_overall_score(role_score, skills_score)"

new = """        # Overall score — 10-dimensional
        overall, _ = matcher.score_10d(
            title=title,
            description=desc_clean,
            role_score=role_score,
            skills_score=skills_score,
            salary_min=float(salary_min) if salary_min else None,
            salary_max=float(salary_max) if salary_max else None,
            posted_date=posted_date,
        )"""

if old in content:
    content = content.replace(old, new)
    with open("dags/job_search_dag.py", "w") as f:
        f.write(content)
    print("SUCCESS: DAG patched — compute_overall_score replaced with score_10d()")
else:
    print("NOT FOUND — printing nearby lines to diagnose:")
    for i, line in enumerate(content.split("\n"), 1):
        if "overall" in line.lower() and "score" in line.lower():
            print(f"  line {i}: {repr(line)}")
