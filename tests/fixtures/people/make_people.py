"""Write made-up spreadsheets with the real people-source headers and planted edge cases.

    python tests/fixtures/people/make_people.py

Every name, email and phone below is invented.
"""

import json
import random
from pathlib import Path

HERE = Path(__file__).parent
rng = random.Random(7)
FIRST = ["Ayu", "Budi", "Citra", "Dimas", "Eka", "Fajar", "Gita", "Hadi", "Intan", "Joko", "Kirana", "Lukman", "Maya",
         "Nanda", "Oka", "Putri", "Raka", "Sari", "Tegar", "Utami", "Vino", "Wulan", "Yoga", "Zahra"]
LAST = ["Pratama", "Wijaya", "Saputra", "Lestari", "Nugroho", "Hidayat", "Kusuma", "Santoso", "Rahman", "Permata"]


def person(i):
    first, last = FIRST[i % len(FIRST)], LAST[(i * 7) % len(LAST)]
    return f"{first} {last}", f"{first.lower()}.{last.lower()}{i}@gmail.com", f"+62812{10000000 + i * 37:08d}"


POOL = [person(i) for i in range(120)]

MENTEE_HDR = ["batch", "email", "phone", "is_selected_mentee", "full_name", "linked_in", "place_of_birth", "Est Age", "",
              "birthday", "current_residence", "application_1_country", "application_1_university", "bachelor_institution"]
MENTOR_HDR = ["batch", "email", "phone", "is_selected_mentor", "full_name", "linkedin", "bachelor_country", "master_country",
              "master_institution", "master_area_of_study"]
# Spelling variants on purpose: the gold step must group them.
RESIDENCE = ["Jakarta Selatan", "DKI Jakarta", "Kota Bandung", "Bandung, Jawa Barat", "Surabaya", "Jogja", "Yogyakarta / DIY",
             "Medan", "Makassar", "Depok", "Tangerang Selatan", "London, UK"]
BIRTH = ["Jakarta", "Bandung", "Surabaya", "Medan", "Padang", "Semarang"]
TARGET = [("United Kingdom", "University of Edinburgh"), ("UK", "UCL"), ("Belanda", "Leiden University"),
          ("USA", "Stanford University"), ("Australia", "University of Melbourne"), ("Japan", "University of Tokyo")]
STUDIED = [("United Kingdom", "University of Oxford", "Economics"), ("Netherlands", "TU Delft", "Engineering"),
           ("Amerika Serikat", "Columbia University", "Public Policy"), ("Australia", "ANU", "Public Policy"),
           ("Germany", "TU Munich", "Engineering")]


def mentee_row(p, year, sel, i=0):
    t = TARGET[i % len(TARGET)]
    return [str(year), p[1], p[2], "TRUE" if sel else "FALSE", p[0], "", BIRTH[i % len(BIRTH)], "", "", "1/2/1998",
            RESIDENCE[i % len(RESIDENCE)], t[0], t[1], "Universitas Indonesia"]


def mentor_row(p, year, sel, i=0):
    st = STUDIED[i % len(STUDIED)]
    return [str(year), p[1], p[2], "TRUE" if sel else "FALSE", p[0], "", "Indonesia", st[0], st[1], st[2]]


# ---- [DB] All Time Mentorship Profiling (Mentee / Mentor)
mentee = [MENTEE_HDR] + [mentee_row(POOL[i], 2019 + i % 6, i % 3 == 0, i) for i in range(60)]
mentor = [MENTOR_HDR] + [mentor_row(POOL[i], 2020 + i % 5, True, i) for i in range(60, 85)]
# Journey: mentee 2021 (selected) becomes mentor 2024, under a Gmail dot variant of the same address.
j = POOL[3]
mentor.append(mentor_row((j[0], j[1].replace(".", "", 1).upper(), j[2]), 2024, True))
# Mentor and mentee in the same year (kept apart, goes to review).
mentor.append(mentor_row(POOL[6], 2019 + 6 % 6, True))
# Duplicate submission in the same source and year.
mentee.append(mentee_row(POOL[10], 2019 + 10 % 6, False))
# One person, two addresses: same name and phone, different emails.
mentee.append(["2025", "rarakinasih@gmail.com", "6287700000001", "TRUE", "Rara Kinasih", "", "", "", "", "", ""])
mentee.append(["2025", "rara.k.work@yahoo.com", "6287700000001", "TRUE", "Rara Kinasih", "", "", "", "", "", ""])
# Shared phone, different people.
mentee.append(["2023", "rina.a@gmail.com", "+6281300000099", "FALSE", "Rina Agustina", "", "", "", "", "", ""])
mentee.append(["2023", "tono.b@gmail.com", "+6281300000099", "FALSE", "Tono Baskoro", "", "", "", "", "", ""])
# Namesakes: same full name, different emails and phones.
mentee.append(["2022", "dewi.lestari@gmail.com", "+6281311111111", "FALSE", "Dewi Lestari", "", "", "", "", "", ""])
mentee.append(["2024", "dlestari@ugm.ac.id", "+6281322222222", "FALSE", "Dewi Lestari", "", "", "", "", "", ""])
# Dummy and broken rows.
mentee += [
    ["2024", "test@example.com", "08123456789", "FALSE", "Test Test", "", "", "", "", "", ""],
    ["2024", "asdf@gmail.com", "0000000000", "FALSE", "asdf", "", "", "", "", "", ""],
    ["2024", "aaaa@yahoo.com", "+62811", "FALSE", "aaaa", "", "", "", "", "", ""],
    ["2024", "budi at gmail", "08129999999", "FALSE", "Budi Real", "", "", "", "", "", ""],
    ["2024", "", "08128888888", "FALSE", "No Email Person", "", "", "", "", "", ""],
    ["2008", "old.timer@gmail.com", "#ERROR!", "FALSE", "Old Timer", "", "", "", "", "", ""],
    ["2023", "two.phones@gmail.com", "+6281100005123 (WA only) / 081100007345", "FALSE", "Dua Telepon", "", "", "", "", "", ""],
    ["2023", "digit.name@gmail.com", "+6281277777777", "FALSE", "Siti Nur 2", "", "", "", "", "", ""],
]
all_time = {"spreadsheet_id": "1nHoX0NFpKD4XIthfgCQ9LU5GFb4B07WpX8UMLlCWfbs", "title": "[DB] All Time Mentorship Profiling",
            "tabs": {"Mentee": mentee, "Mentor": mentor}}

# ---- Participant Database: events, forms, PhD mentee
events = [["event_date", "event_name", "event_role", "is_online_event", "email", "phone", "full_name", "reference_link"]]
for i in range(85, 115):
    events.append([f"{2018 + i % 5}-03-0{1 + i % 8}", "IM Gathering", "Participant", "FALSE", POOL[i][1], POOL[i][2], POOL[i][0], ""])
for i in range(0, 12):  # event participants who later became mentees
    events.append(["2018-05-01", "EHEF 2018", "Participant", "FALSE", POOL[i][1], POOL[i][2], POOL[i][0], ""])
for i in range(60, 66):  # mentors who spoke at events
    events.append(["2025-02-01", "Webinar PhD", "Speaker", "TRUE", POOL[i][1], POOL[i][2], POOL[i][0], ""])
events.append(["2019-04-01", "IM Gathering", "Management", "FALSE", POOL[1][1], POOL[1][2], POOL[1][0], ""])
phd = [["Batch", "Timestamp", "Email Address", "Full Name", "Place of Birth", "Date of Birth", "WhatsApp Phone Number",
        "Your Current Residency (Region)", "Your Current Residency (City/ Regency)", "Target Country"]]
for i in range(20, 32):
    phd.append(["2024", "1/3/2024 10:00:00", POOL[i][1], POOL[i][0], "Bandung", "1/1/1995", POOL[i][2],
                "Jawa Timur" if i % 2 else "Sulawesi Selatan", "Malang" if i % 2 else "", "Netherlands"])
pdb = {"spreadsheet_id": "1W8VrZeyT1HKyZ1KQqI6tVOfw08WZ6-8zZ0eCO_eagFM", "title": "Participant Database",
       "tabs": {"event_participants": events, "PhD_mentee": phd,
                "mentee_registration_form": [MENTEE_HDR[:5]] + [mentee_row(POOL[i], 2023, False)[:5] for i in range(0, 8)],
                "mentor_registration_form": [MENTOR_HDR[:5]] + [mentor_row(POOL[i], 2023, True)[:5] for i in range(60, 66)]}}

# ---- Management
accepted = [["", "", "", "", "", "", "Unique Members", "9", "2"], ["Year", "Email", "Division", "Position", "Count"]]
for i in list(range(0, 8)) + [1]:
    accepted.append([str(2022 + i % 3), POOL[i][1], "Data", "Member", "1"])
applied = [[], ["Year", "Email", "International Exp", "Past IM Exp", "Position Choice 1"]]
for i in range(0, 14):
    applied.append([str(2022 + i % 3), POOL[i][1], "Yes", "Mentee", "Data"])
mgmt = {"spreadsheet_id": "1XU3OVciel1WwKxwEQGj14X2J0Faxnw-wKRRTF7n6Rwc", "title": "[RAW] IM Management Team 2020 - 2025",
        "tabs": {"Accepted": accepted, "Applied": applied}}
selected = {"spreadsheet_id": "14957JP6j5OBdFC5rIorWzK31UuQSnmF3d1Lx2rLbXX0", "title": "[RAW] IM Management Team - SELECTED",
            "tabs": {"Sheet1": [["Year", "Location", "Email", "Name", "Division", "Role"]]
                     + [["2025", "Jakarta", POOL[i][1], POOL[i][0], "Data", "Analyst"] for i in range(0, 5)]}}

# ---- PhD bootcamp (has a Dummy_ column) and PhD 2026 mentors (contract sheet)
bootcamp = {"spreadsheet_id": "1jvWpB1M7uE18wRh0szgqimQptLI1AMqv2f66D_zR4zw", "title": "phd bootcamp",
            "tabs": {"final_participant": [["ID", "email", "research", "university", "Dummy_Motivation"]]
                     + [[str(k), POOL[i][1], "AI", "UI", "lorem"] for k, i in enumerate(range(20, 26))],
                     "final_coach": [["email", "research", "university", "country"]] + [[POOL[i][1], "AI", "NUS", "Singapore"] for i in range(66, 72)],
                     "final result": [["Emails", "group_name", "matching_score", "mentor_email", "mentor_name", "mentee_ID", "mentee_email", "mentee_name"]]
                     + [["", f"Group {k % 3 + 1}", "0.8", POOL[66 + k % 3][1].upper() if k == 0 else POOL[66 + k % 3][1], POOL[66 + k % 3][0],
                         str(k), POOL[20 + k][1], POOL[20 + k][0]] for k in range(6)]}}
mentor_phd = {"spreadsheet_id": "1hUpoba7UJ0qQgX10GWsYlNewfVILi44YgULhLHSM-wc", "title": "Mentor_Data_PhD_Mentorship_2026",
              "tabs": {"DATA": [["Timestamp", "Email Address", "Full Name", "Phone Number (WhatsApp)"]]
                       + [["9/1/2026 10:00:00", POOL[i][1], POOL[i][0], POOL[i][2]] for i in range(66, 74)],
                       "COLUMN_METADATA": [["no", "column_name", "column_description", "column_header_location", "form_question"],
                                           ["1", "timestamp", "", "A1", "Timestamp"], ["2", "email_address", "", "B1", "Email Address"],
                                           ["3", "full_name", "", "C1", "Full Name"], ["4", "phone_number_whatsapp", "", "D1", "Phone"]]}}
# ---- Master pairing: ids only; the raw tabs give names (and mentor phones).
raw_mentor = [["Timestamp", "id", "selected_mentor", "Are you past IM Mentor?", "Full Name", "Linkedin URL", "Phone number (WhatsApp)"]]
raw_mentor += [["", f"MR-{j}", "TRUE", "No", POOL[60 + j][0], "", POOL[60 + j][2]] for j in range(5)]
raw_mentor.append(["", "MR-5", "TRUE", "No", POOL[65][0], "", ""])  # no phone: matched by name
raw_mentee = [["Full Name", "id", "selected_for_pairing", "area_of_study", "region"]]
raw_mentee += [[POOL[j][0], f"ME-{j}", "TRUE", "Engineering", "Europe"] for j in range(15)]
raw_mentee += [["Dewi Lestari", "ME-90", "TRUE", "", ""], ["Nobody Known", "ME-91", "TRUE", "", ""]]
final_pairing = [["Matching Score", "mentor_id", "mentor_faculty", "mentee_id", "mentee_faculty"]]
final_pairing += [["0.9", f"MR-{j % 6}", "", f"ME-{j}", ""] for j in range(15)]
final_pairing += [["0.5", "MR-0", "", "ME-90", ""], ["0.5", "MR-1", "", "ME-91", ""]]  # ambiguous and unknown mentee
pairing = {"spreadsheet_id": "1Z-lly28ZecwG3Qbs5oz5YRSyx-jfIUZQbzchmjd-UEg", "title": "mentee mentor pairing",
           "tabs": {"final_pairing": final_pairing, "raw mentor": raw_mentor, "raw mentee": raw_mentee}}

# Mentee_new_2025, Mentor_new_2025, Mentee_2024, mentor_legacy_2024, PhD mentee 2026 and Management Recruitment are
# left out on purpose: those sources show up as unreachable.

for name, book in [("all_time", all_time), ("participant_db", pdb), ("management", mgmt), ("management_selected", selected),
                   ("phd_bootcamp", bootcamp), ("mentor_phd_2026", mentor_phd), ("pairing", pairing)]:
    (HERE / f"{name}.json").write_text(json.dumps(book, indent=1) + "\n")
