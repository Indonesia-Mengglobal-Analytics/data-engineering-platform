"""Write demo fixtures: the PhD mentorship sheet's real data dictionary with made-up rows.

    python tests/fixtures/make_demo.py

No real applicant data is used; every value below is invented.
"""

import json
import random
from pathlib import Path

HERE = Path(__file__).parent

COLUMNS = [
    ("timestamp", "Date and time when the form submission was received.", "Timestamp"),
    ("email_address", "Primary email address provided by the mentee applicant for communication.", "Email Address"),
    ("full_name", "Legal or full name of the mentee applicant.", "Full Name"),
    ("date_of_birth", "Applicant's date of birth.", "Date of Birth"),
    ("whatsapp_phone_number", "Applicant's phone number used for WhatsApp communication.", "WhatsApp Phone Number"),
    ("are_you_an_indonesian_citizen", "Citizenship status indicator for the applicant.", "Are you an Indonesian citizen?"),
    ("your_current_residency_province", "Province of current residence within Indonesia (if applicable).", "Your Current Residency (Province)"),
    ("your_current_residency_city_regency", "City or regency of current residence.", "Your Current Residency (City/ Regency)"),
    ("current_occupation_job_sector", "Industry or professional sector of the applicant's current job.", "Current Occupation - Job Sector"),
    ("current_occupation_institution", "Employer, university, or company name for current occupation.", "Current Occupation - Institution"),
    ("bachelors_degree_name_of_institution", "University or college where the applicant earned their undergraduate degree.", "Bachelor's Degree - Name of Institution"),
    ("bachelors_degree_url_of_institution", "Official website link for the bachelor's degree institution.", "Bachelor's Degree - URL of institution"),
    ("bachelors_degree_country", "Country where the applicant's undergraduate institution is located.", "Bachelor's Degree - Country"),
    ("bachelors_degree_area_of_study", "Broad academic field of study for the bachelor's degree.", "Bachelor's Degree - Area of Study"),
    ("bachelors_degree_major", "Specific major or field of concentration for the bachelor's degree.", "Bachelor's Degree - Major"),
    ("bachelors_degree_graduation_date", "Graduation date or completion year for the bachelor's degree.", "Bachelor's Degree - Graduation Date"),
    ("masters_degree_name_of_institution", "University or college where the applicant earned their master's degree (if applicable).", "Master's Degree - Name of Institution"),
    ("masters_degree_url_of_institution", "Official website link for the master's degree institution.", "Master's Degree - URL of Institution"),
    ("masters_degree_country", "Country where the applicant's master's degree institution is located.", "Master's Degree - Country"),
    ("masters_degree_area_of_study", "Broad academic field of study for the master's degree.", "Master's Degree - Area of Study"),
    ("masters_degree_major", "Specific major or field of concentration for the master's degree.", "Master's Degree - Major"),
    ("masters_degree_graduation_date", "Graduation date or completion year for the master's degree.", "Master's Degree - Graduation Date"),
    ("when_do_you_plan_to_start_your_phd", "Target start date for commencing doctoral studies.", "When do you plan to start your PhD? (MM/YYYY)"),
    ("which_regions_are_you_interested_in_studying_in", "Geographic regions targeted for prospective PhD applications.", "Which regions are you interested in studying in?"),
    ("preferred_country_of_study", "Primary target country or countries for PhD applications.", "Preferred country of study"),
    ("preferred_university_or_institution_1", "First-choice target university or institution for PhD study.", "Preferred University or Institution 1"),
    ("preferred_university_1_url_of_institution", "Official website URL for the first preferred university.", "Preferred University 1 - URL of Institution"),
    ("preferred_university_or_institution_2", "Second-choice target university or institution for PhD study.", "Preferred University or Institution 2"),
    ("preferred_university_2_url_of_institution", "Official website URL for the second preferred university.", "Preferred University 2 - URL of Institution"),
    ("preferred_university_or_institution_3", "Third-choice target university or institution for PhD study.", "Preferred University or Institution 3"),
    ("preferred_university_3_url_of_institution", "Official website URL for the third preferred university.", "Preferred University 3 - URL of Institution"),
    ("phd_area_of_study", "Broad academic field characterizing the target PhD program.", "Which area of study best characterized the PhD program that you're planning to apply to?"),
    ("phd_specialization_research_focus", "Specific area of sub-specialization or primary research focus for PhD study.", "PhD Specialization/ Research Focus"),
    ("funding_plan_name_of_scholarship", "Name of targeted or secured scholarship/funding scheme.", "Funding Plan - Name of Scholarship"),
    ("funding_plan_detail_of_scholarship", "Additional details regarding the applicant's PhD funding plan.", "Funding Plan - Detail of Scholarship"),
    ("language_proficiency_test_taken", "Standardized English/language test details (e.g., test name, score, year taken).", "Language proficiency test that you have taken (name of the test, score, year taken)"),
    ("prior_research_experience", "Binary or categorical indicator of previous research experience.", "Do you have any prior research experience?"),
    ("meaningful_research_experience_details", "Narrative detailing the applicant's most impactful prior research experience.", "If yes, please tell us about your most meaningful research experience and why it was significant to you. (Max 1000 characters)"),
    ("link_to_your_research_profile", "Web link to academic profiles, publication listings, or research portfolios.", "Link to your research profile if any (e.g., Google Scholar, ResearchGate, Academia.edu)"),
    ("phd_application_preparations", "Summary of action steps already taken toward applying for a PhD.", "Please list any preparations you have undertaken for applying to a PhD program. (Max 1200 characters)"),
    ("motivation_for_pursuing_phd", "Personal statement explaining core motivations for pursuing a doctoral degree.", "Tell us your motivation of pursuing PhD (600 - 1200 characters)"),
    ("hopes_to_learn_from_mentorship", "Specific learning objectives and expectations from the mentorship program.", "If selected as a mentee, what are the things you wish to learn from this mentorship? (Max 1200 characters)"),
    ("research_proposal_draft", "Preliminary draft or abstract outlining the proposed PhD research topic and strategy.", "Write down a short draft of your research proposal (1200 - 2400 characters)"),
    ("video_motivation_link", "URL link to a self-recorded motivation video submitted by the applicant.", "Please provide a link to a 2–3 minute video explaining your motivation for joining the mentorship."),
    ("program_schedule_commitment", "Formal declaration agreeing to attend mandatory session dates and complete tasks.", "You declare that you are committed to submit all assignments and virtually attend the entire PhD Mentorship module on these dates:"),
    ("referral_source", "Promotional channel or platform where the applicant learned about the mentorship program.", "Where did you hear about Indonesia Mengglobal PhD Mentorship from?"),
    ("management_team_connection", "Disclosure of personal acquaintance with any current Indonesia Mengglobal team member.", "If you know anyone personally in Indonesia Mengglobal's current Management Team, please list his/her name."),
    ("publication_consent", "Consent to publish applicant's name and testimonial upon program completion.", "Are you willing to share your name and testimonial for publication?"),
    ("require_special_assistance_or_accommodation", "Indicator of whether accessibility assistance or special accommodation is required.", "Would you require any special assistance or accommodation?"),
    ("special_assistance_details", "Detailed explanation of required accessibility accommodations.", "Please provide more details on the type of special assistance or accommodation required"),
    ("applicant_declarations", "Multiple-choice acknowledgment of program terms, accuracy, privacy, and application commitment.", "You declare that:"),
    ("legal_signature", "Legal name typed by the applicant acting as a legal digital signature.", "By signing (typing your legal name) in the space below, you are certifying that all information is correct"),
    ("privacy_policy_consent", "Explicit consent agreeing to the program's privacy policy and data processing terms.", "By submitting this form, I confirm that I have read and agreed to the Privacy Policy of Indonesia Mengglobal."),
]

CHOICES = {
    "are_you_an_indonesian_citizen": ["Yes"] * 19 + ["No"],
    "your_current_residency_province": ["DKI Jakarta"] * 6 + ["Jawa Barat"] * 5 + ["Jawa Timur"] * 3 + ["DI Yogyakarta"] * 3 + ["Sulawesi Selatan", "Bali", "Aceh"],
    "current_occupation_job_sector": ["Education / Academia"] * 6 + ["Government"] * 3 + ["Private sector"] * 4 + ["NGO"] * 2 + ["Student"] * 3,
    "bachelors_degree_country": ["Indonesia"] * 9 + ["Malaysia", "Australia"],
    "bachelors_degree_area_of_study": ["Engineering", "Social Sciences", "Natural Sciences", "Health Sciences", "Economics & Business"],
    "bachelors_degree_major": ["Civil Engineering", "Public Health", "Economics", "Biology", "Psychology", "Computer Science"],
    "masters_degree_country": ["Indonesia", "United Kingdom", "Australia", "Netherlands", "Japan", ""],
    "masters_degree_area_of_study": ["Engineering", "Social Sciences", "Natural Sciences", "Health Sciences", ""],
    "masters_degree_major": ["Public Policy", "Environmental Science", "Data Science", "Epidemiology", ""],
    "when_do_you_plan_to_start_your_phd": ["09/2027", "09/2027", "01/2028", "09/2028"],
    "which_regions_are_you_interested_in_studying_in": ["Europe", "Europe, North America", "Asia Pacific", "Australia & New Zealand"],
    "preferred_country_of_study": ["United Kingdom"] * 4 + ["Australia"] * 3 + ["Netherlands"] * 2 + ["United States"] * 2 + ["Japan", "Germany"],
    "phd_area_of_study": ["Engineering", "Social Sciences", "Natural Sciences", "Health Sciences", "Economics & Business", "Humanities"],
    "funding_plan_name_of_scholarship": ["LPDP"] * 8 + ["Chevening", "Australia Awards", "Fulbright", "University scholarship"],
    "prior_research_experience": ["Yes"] * 3 + ["No"],
    "program_schedule_commitment": ["Yes, I am committed"],
    "referral_source": ["Instagram"] * 5 + ["Friends / colleagues"] * 3 + ["LinkedIn"] * 2 + ["Indonesia Mengglobal website", "X / Twitter"],
    "publication_consent": ["Yes"] * 4 + ["No"],
    "require_special_assistance_or_accommodation": ["No"] * 15 + ["Yes"],
    "applicant_declarations": ["I agree to all of the above"],
    "privacy_policy_consent": ["I agree"] * 30 + [""],
}


def column_location(index: int) -> str:
    letters = ""
    index += 1
    while index:
        index, rem = divmod(index - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}1"


def make_rows(count: int, rng: random.Random) -> list[list[str]]:
    rows = []
    for i in range(count):
        day = 6 + int(21 * (i / count) ** 1.6)  # submissions pile up near the deadline
        row = []
        for name, _, _ in COLUMNS:
            if name == "timestamp":
                value = f"7/{day}/2026 {rng.randint(7, 23)}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}"
            elif name == "email_address":
                value = f"applicant{i if i % 37 else i - 1}@example.com"  # a few resubmissions
            elif name in ("full_name", "legal_signature"):
                value = f"Demo Applicant {i}"
            elif name == "date_of_birth":
                value = f"{rng.randint(1, 28)}/{rng.randint(1, 12)}/{rng.randint(1988, 2000)}"
            elif name == "whatsapp_phone_number":
                value = f"+62 812 0000 {i:04d}"
            elif name in CHOICES:
                value = rng.choice(CHOICES[name])
            elif "url" in name or "link" in name:
                value = "https://example.com" if rng.random() > 0.3 else ""
            else:
                value = "Demo answer." if rng.random() > 0.1 else ""
            row.append(value)
        rows.append(row)
    return rows


def main() -> None:
    rng = random.Random(2026)
    column_metadata = [["no", "column_name", "column_description", "column_header_location", "form_question"]]
    column_metadata += [[str(i + 1), n, d, column_location(i), q] for i, (n, d, q) in enumerate(COLUMNS)]
    header = [q for _, _, q in COLUMNS]
    header[38] += "  "  # the real sheet has stray spaces in this header; the contract must tolerate them
    table_metadata = [
        ["table_name", "table_description", "owner_email", "operation_date_start", "operation_date_end", "populated_date", "purpose", "version"],
        ["data", "Applications to the Indonesia Mengglobal PhD Mentorship Program (demo rows).", "phd@indonesiamengglobal.com",
         "20260706", "20260727", "20260909", "Select mentees and match them with mentors.", "1"],
        ["column_metadata", "Data dictionary for the data tab.", "phd@indonesiamengglobal.com", "20260706", "20260727", "20260909",
         "Explains every column.", "1"],
    ]
    demo = {
        "spreadsheet_id": "demo-phd-mentorship-2026",
        "title": "Mentee_Data_PhD_Mentorship_2026 (demo)",
        "modified_time": "2026-10-24T13:13:13Z",
        "tabs": {"DATA": [header] + make_rows(148, rng), "COLUMN_METADATA": column_metadata, "TABLE_METADATA": table_metadata},
    }
    rejected = {
        "spreadsheet_id": "demo-rejected-volunteers",
        "title": "Volunteer_Signups_2026 (demo, wrong format)",
        "modified_time": "2026-10-20T08:00:00Z",
        "tabs": {"Form Responses 1": [["Timestamp", "Name", "Division"], ["7/1/2026 10:00:00", "Demo", "Media"]]},
    }
    for name, payload in (("demo_phd_mentorship.json", demo), ("demo_rejected.json", rejected)):
        (HERE / name).write_text(json.dumps(payload, ensure_ascii=False, indent=1) + "\n")


if __name__ == "__main__":
    main()
