import sqlite3
import os
import random

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'pecan.db')

FIRST_NAMES = [
    "Emma", "Oliver", "Amara", "James", "Sophia", "Liam", "Priya", "Noah", "Fatima", "William",
    "Ava", "Benjamin", "Zara", "Lucas", "Isla", "Henry", "Aisha", "Alexander", "Mia", "Daniel",
    "Charlotte", "Samuel", "Yuki", "Jack", "Olivia", "Leo", "Nadia", "Oscar", "Grace", "Ethan",
    "Chloe", "Ryan", "Sara", "Thomas", "Hannah", "Ahmed", "Elena", "Marcus", "Lily", "Kai",
    "Ruby", "Finn", "Jasmine", "Hugo", "Leah", "Arjun", "Maya", "Ravi", "Tara", "Chen",
]

LAST_NAMES = [
    "Chen", "Williams", "Patel", "Brown", "Singh", "Taylor", "Kim", "Anderson", "Okafor", "Martinez",
    "Thompson", "Garcia", "Nguyen", "Robinson", "Ali", "Clark", "Das", "Walker", "Yamamoto", "Hall",
    "Lee", "Wright", "Khan", "Moore", "Campbell", "Rivera", "Adams", "Mitchell", "Reeves", "Scott",
    "Murphy", "Cox", "Brooks", "Hughes", "Foster", "Reed", "Shaw", "Bell", "Price", "Cooper",
    "Ward", "Morgan", "Evans", "Collins", "Stewart", "Powell", "Rao", "Shah", "Li", "Zhang",
]

DEPARTMENTS = [
    "Economics", "Computer Science", "Finance", "Law", "Business Administration",
    "Engineering", "Medicine", "Psychology", "Data Science", "Politics",
    "Mathematics", "Marketing", "Architecture", "Biology", "History",
    "Chemistry", "Philosophy", "English Literature", "Mechanical Engineering", "International Relations",
]

UNIVERSITIES = [
    "UCL", "Bristol", "Edinburgh", "Manchester", "Kings College London",
    "Imperial College", "Leeds", "Birmingham", "Glasgow", "Warwick",
]

CITIES = [
    "London", "Bristol", "Edinburgh", "Manchester", "Birmingham",
    "Leeds", "Glasgow", "Oxford", "Cambridge", "Liverpool",
    "Nottingham", "Sheffield", "Cardiff", "Belfast", "Brighton",
]

INDUSTRIES = [
    "Finance", "Technology", "Consulting", "Healthcare", "Law",
    "Education", "Media", "Engineering", "Government", "Non-profit",
    "Real Estate", "Retail", "Energy", "Pharma", "Marketing",
]

JOB_TITLES = [
    "Analyst", "Associate", "Manager", "Consultant", "Developer",
    "Director", "Coordinator", "Specialist", "Engineer", "Researcher",
    "VP", "Head of", "Senior Associate", "Principal", "Partner",
]

COMPANIES = [
    "Goldman Sachs", "Deloitte", "Google", "NHS", "Clifford Chance",
    "McKinsey", "Barclays", "BBC", "Rolls-Royce", "Unilever",
    "HSBC", "PwC", "Amazon", "BP", "Teach First",
    "JP Morgan", "EY", "Microsoft", "GSK", "Accenture",
]

INTERESTS_POOL = [
    "fintech", "AI", "sustainability", "entrepreneurship", "healthcare innovation",
    "venture capital", "data science", "public policy", "social impact", "blockchain",
    "climate tech", "edtech", "cybersecurity", "biotech", "creative industries",
    "real estate", "consulting", "law reform", "media", "robotics",
]

EVENT_NAMES = [
    "Fintech Panel 2024", "Healthcare Networking Night", "AI in Business Workshop",
    "Alumni Careers Fair 2023", "Sustainability Summit", "Leadership Gala 2024",
    "Startup Pitch Night", "Women in Tech Panel", "",
]


def generate_alumni(n=250):
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM alumni")
    count = c.fetchone()[0]
    if count >= 200:
        print(f"Alumni already seeded (found {count} records), skipping.")
        conn.close()
        return
    if 1 <= count <= 199:
        print(f"Found {count} existing records. Adding {n} more.")

    for _ in range(n):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        name = f"{first} {last}"
        dept = random.choice(DEPARTMENTS)
        uni = random.choice(UNIVERSITIES)
        grad_year = random.randint(2016, 2025)
        city = random.choice(CITIES)
        industry = random.choice(INDUSTRIES)
        title_base = random.choice(JOB_TITLES)
        company = random.choice(COMPANIES)
        interests = "; ".join(random.sample(INTERESTS_POOL, k=random.randint(2, 4)))
        engagement = random.randint(10, 95)
        email_valid = 0 if random.random() < 0.05 else 1
        gdpr = 0 if random.random() < 0.03 else 1
        past = "; ".join(random.sample(EVENT_NAMES, k=random.randint(0, 3)))
        source = random.choice(["CRM", "Events Team", "Careers Office"])
        email_domain = (
            "gmail.com"
            if random.random() > 0.5
            else f"alumni.{uni.lower().replace(' ', '')}.ac.uk"
        )
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 99)}@{email_domain}"
        degree_type = random.choice(["BSc", "BA", "MSc", "MEng", "LLB"])
        degree = f"{degree_type} {dept}"

        if title_base in ["Head of", "Director", "VP"]:
            job_title = f"{title_base} {random.choice(['Strategy', 'Operations', 'Product', 'Analytics', 'Growth'])}"
        else:
            job_title = title_base

        c.execute(
            """INSERT INTO alumni
            (name, email, graduation_year, degree, department, location_city, location_country,
             job_title, company, industry, interests, engagement_score, email_valid, gdpr_consent,
             past_events, data_source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                name,
                email,
                grad_year,
                degree,
                dept,
                city,
                "UK",
                job_title,
                company,
                industry,
                interests,
                engagement,
                email_valid,
                gdpr,
                past,
                source,
            ),
        )

    conn.commit()
    conn.close()
    print(f"Generated {n} alumni records.")


if __name__ == "__main__":
    generate_alumni(250)
