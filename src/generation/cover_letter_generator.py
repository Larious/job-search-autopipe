"""
Cover Letter Generator for Job Search AutoPipe.

Generates tailored cover letters by combining the user's skills profile
with specific job description requirements. Uses Claude API for generation,
with a fallback template-only mode.

This is Phase 4 of the pipeline — it only runs for jobs the user has
explicitly flagged after reviewing the daily digest.
"""

import json
import logging
from datetime import datetime
from typing import Optional
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


COVER_LETTER_SYSTEM_PROMPT = """You are a professional cover letter writer for data engineering roles.

RULES:
1. Write in first person, professional but warm tone
2. Keep it to 3-4 paragraphs, under 400 words
3. Opening: Reference the specific role and company — show you've read the JD
4. Middle: Connect 2-3 of the candidate's strongest matching skills to specific JD requirements
5. Include ONE concrete project example that demonstrates relevant experience
6. Closing: Express enthusiasm and mention availability
7. Do NOT use generic filler phrases like "I am writing to express my interest"
8. Do NOT exaggerate — only reference skills the candidate actually has
9. Sound human, not templated — vary sentence structure and length
10. If the JD mentions tools the candidate knows, reference hands-on experience with them

Output ONLY the cover letter text. No subject line, no "Dear Hiring Manager" header,
no sign-off — just the body paragraphs. The formatting wrapper is handled separately."""


class CoverLetterGenerator:
    """
    Generates tailored cover letters for flagged job applications.
    
    Supports three engines:
    - claude_api: Uses Anthropic's Claude API (recommended)
    - ollama: Uses a local Ollama instance
    - template_only: Simple string template (no AI, always available)
    """

    def __init__(self, config: dict, skills_profile: dict):
        self.config = config
        self.profile = skills_profile
        self.engine = config.get("engine", "template_only")

    def generate(self, job: dict, matched_skills: list = None,
                 missing_skills: list = None) -> str:
        """
        Generate a tailored cover letter for a specific job.
        
        Args:
            job: Dict with keys: title, company, location, description_clean, url
            matched_skills: Skills from the user's profile found in the JD
            missing_skills: Skills in the JD the user doesn't have
            
        Returns:
            Cover letter text (body paragraphs only)
        """
        if self.engine == "claude_api":
            return self._generate_claude(job, matched_skills, missing_skills)
        elif self.engine == "ollama":
            return self._generate_ollama(job, matched_skills, missing_skills)
        else:
            return self._generate_template(job, matched_skills)

    def _build_prompt(self, job: dict, matched_skills: list = None,
                      missing_skills: list = None) -> str:
        """Build the generation prompt with all context."""
        personal = self.profile.get("personal", {})
        projects = self.profile.get("projects", [])

        # Format project summaries
        project_text = ""
        for p in projects[:2]:
            techs = ", ".join(p.get("technologies", [])[:6])
            project_text += f"\n- {p['name']}: {p['description'].strip()} (Tech: {techs})"

        prompt = f"""Generate a tailored cover letter for this role:

JOB DETAILS:
- Title: {job.get('title', 'Data Engineer')}
- Company: {job.get('company', 'Unknown')}
- Location: {job.get('location', '')}
- Description: {job.get('description_clean', '')[:2000]}

CANDIDATE PROFILE:
- Name: {personal.get('name', 'Candidate')}
- Location: {personal.get('location', 'Glasgow, Scotland')}
- Target role: {personal.get('title', 'Data Engineer')}

MATCHING SKILLS (candidate HAS these, mentioned in JD):
{', '.join(matched_skills or [])}

MISSING SKILLS (in JD but candidate doesn't have):
{', '.join(missing_skills or [])}

PORTFOLIO PROJECTS:{project_text}

IMPORTANT: Focus on the matching skills and project experience. 
Do NOT claim proficiency in the missing skills — instead, show willingness to learn.
Reference specific tools from the JD that the candidate has used in their projects."""

        return prompt

    def _generate_claude(self, job: dict, matched_skills: list = None,
                         missing_skills: list = None) -> str:
        """Generate using Claude API."""
        api_config = self.config.get("claude_api", {})
        api_key = api_config.get("api_key", "")
        model = api_config.get("model", "claude-sonnet-4-20250514")

        if not api_key:
            logger.warning("No Claude API key configured, falling back to template")
            return self._generate_template(job, matched_skills)

        prompt = self._build_prompt(job, matched_skills, missing_skills)

        payload = json.dumps({
            "model": model,
            "max_tokens": 1024,
            "system": COVER_LETTER_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": prompt}],
        }).encode()

        req = Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )

        try:
            with urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())

            # Extract text from response
            content = data.get("content", [])
            text_parts = [block["text"] for block in content if block.get("type") == "text"]
            letter = "\n\n".join(text_parts)

            logger.info(f"Cover letter generated via Claude API ({len(letter)} chars)")
            return letter

        except Exception as e:
            logger.error(f"Claude API error: {e}. Falling back to template.")
            return self._generate_template(job, matched_skills)

    def _generate_ollama(self, job: dict, matched_skills: list = None,
                         missing_skills: list = None) -> str:
        """Generate using local Ollama instance."""
        ollama_config = self.config.get("ollama", {})
        model = ollama_config.get("model", "llama3")
        base_url = ollama_config.get("base_url", "http://localhost:11434")

        prompt = self._build_prompt(job, matched_skills, missing_skills)

        payload = json.dumps({
            "model": model,
            "prompt": f"{COVER_LETTER_SYSTEM_PROMPT}\n\n{prompt}",
            "stream": False,
        }).encode()

        req = Request(
            f"{base_url}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode())
            letter = data.get("response", "")
            logger.info(f"Cover letter generated via Ollama ({len(letter)} chars)")
            return letter
        except Exception as e:
            logger.error(f"Ollama error: {e}. Falling back to template.")
            return self._generate_template(job, matched_skills)

    def _generate_template(self, job: dict, matched_skills: list = None) -> str:
        """
        Fallback: simple template-based cover letter.
        No AI required — always works.
        """
        personal = self.config.get("your_details", {})
        name = personal.get("full_name", "Candidate")
        projects = self.profile.get("projects", [])
        project_name = projects[0]["name"] if projects else "a production data pipeline"

        skills_text = ", ".join(matched_skills[:5]) if matched_skills else "Python, SQL, and modern data tools"

        letter = f"""The {job.get('title', 'Data Engineer')} role at {job.get('company', 'your company')} caught my attention because it aligns closely with the work I've been doing building production data pipelines. Having worked extensively with {skills_text}, I'm confident I can contribute meaningfully to your data engineering team.

My most relevant experience is {project_name}, an end-to-end data pipeline I designed and built from scratch. This project uses a medallion architecture (bronze/silver/gold layers) in PostgreSQL, orchestrated with Apache Airflow, with dbt Core for transformations and Great Expectations for automated data quality validation. The pipeline handles API ingestion, web scraping, change detection via SHA-256 hashing, and publishes cleaned data through a REST API — demonstrating the full lifecycle of data engineering work.

I'm particularly drawn to this role because of the opportunity to apply these patterns at scale. I'm a self-directed learner who builds real systems to develop skills, and I'm eager to bring that energy to a team where I can both contribute and grow.

I'd welcome the chance to discuss how my hands-on pipeline experience maps to your team's needs. I'm available for a conversation at your convenience."""

        logger.info(f"Cover letter generated via template ({len(letter)} chars)")
        return letter

    def format_full_letter(self, body: str, job: dict) -> str:
        """Wrap the letter body with proper formatting for PDF/email output."""
        personal = self.config.get("your_details", {})
        name = personal.get("full_name", "Candidate")
        email = personal.get("email", "")
        phone = personal.get("phone", "")
        location = personal.get("location", "Glasgow, Scotland")
        date_str = datetime.now().strftime("%d %B %Y")

        header = f"""{name}
{location}
{email} | {phone}

{date_str}

Re: {job.get('title', 'Data Engineer')} — {job.get('company', '')}

Dear Hiring Manager,

"""
        footer = f"""

Yours sincerely,
{name}"""

        return header + body + footer
