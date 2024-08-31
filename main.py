from basicauth import decode
from typing import List, Optional
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ClientError
from pydantic import BaseModel, Field
import functions_framework
from openai import OpenAI
import json
import os
import logging

# Note: Pydantic does not appear to work properly in Google Cloud Functions

HOST = os.environ.get("NEO4J_URI")
PASSWORD = os.environ.get("NEO4J_PASSWORD")
USER = os.environ.get("NEO4J_USER", "neo4j")
DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")
OPENAI_KEY = os.environ.get("OPENAI_API_KEY")


class Location(BaseModel):
    isCurrent: Optional[bool] = None
    address: Optional[str] = None


class DiffBotTime(BaseModel):
    timestamp: Optional[int] = None

class Employer(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None

class Employment(BaseModel):
    isCurrent: bool
    employer: Optional[Employer] = None
    description: Optional[str] = None
    location: Optional[Location] = None
    title: Optional[str] = None
    to: Optional[DiffBotTime] = None

class Institution(BaseModel):
    name: Optional[str] = None
    diffbotUri: Optional[str] = None

class Education(BaseModel):
    institution: Optional[dict] = None
    isCurrent: Optional[bool] = None
    from_: Optional[DiffBotTime] = None
    to: Optional[DiffBotTime] = None

class DiffBotName(BaseModel):
    firstName: Optional[str] = None
    lastName: Optional[str] = None


class Entity(BaseModel):
    diffbotUri: Optional[str] = None
    nameDetail: Optional[DiffBotName] = None
    description: Optional[str] = None
    origins: List[str] = Field(default_factory=list)
    summary: Optional[str] = None
    languages: List[dict] = Field(default_factory=list)
    educations: List[Education] = Field(default_factory=list)
    employments: List[Employment] = Field(default_factory=list)

    def get_languages(self) -> List[str]:
        return [lang["normalizedValue"] for lang in self.languages]


class Data(BaseModel):
    entity: Entity


class DiffBotData(BaseModel):
    data: List[Data] = Field(default_factory=list)

CLIENT = OpenAI(
    api_key=OPENAI_KEY,
)

def extract_tech(sentence: str) -> list[str]:

    # Return empty list if sentence is empty
    if not sentence or sentence.strip() == "":
        return []

    prompt = """Return a JSON List of any application names, software technologies, or programming languages from a user statement of the following structure: 

    {
    "application": [...list of applications]
    }
    
    For example, return {"application": ["NextJS", "Django", "PostgresSQL"]} from the sentence "NextJS + Django + PostgreSQL" or the sentence "I am most comfortable with NextJS, Django, and PostgresSQL"."""

    response = CLIENT.chat.completions.create(
        model="gpt-4o",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": sentence},
        ],
        max_tokens=200,
        n=1,
        stop=None,
        temperature=0.0,
    )

    extracted = response.choices[0].message.content

    # print(f"Extract topics response from LLM: {extracted}")

    extracted = json.loads(extracted)

    # LLM may return a dictionary instead of the requested list
    if isinstance(extracted, dict):
        aggregate = []
        # LLM may return several lists within a dictionary.
        for key in list(extracted.keys()):
            a_list = extracted[key]

            # Skip any key-value payloads that aren't a list of strings
            if not isinstance(a_list, list) and not all(
                isinstance(item, str) for item in a_list
            ):
                print(
                    f"Skipping dictionary payload '{a_list}' that is not entirely a list of strings."
                )
                continue

            # Aggregate all the results
            aggregate.extend(a_list)

        extracted = aggregate

    if not isinstance(extracted, list) or not all(
        isinstance(item, str) for item in extracted
    ):
        raise ValueError(
            f"Expected list of strings was not returned from the LLM: {extracted}"
        )

    # print(f"Topics extracted: {extracted}")

    return extracted

def execute_query(query, params):
    with GraphDatabase.driver(
        HOST, auth=basic_auth(USER, PASSWORD), database=DATABASE
    ) as driver:
        return driver.execute_query(query, params)



def ingest(dbd: DiffBotData, tenant_id: str):

    # Create new tenant node if it doesn't already exist
    query = """
    MERGE (t:Tenant {name: $tenant_id})
    ON CREATE SET t.created_at = datetime()
    """
    params = {"tenant_id": tenant_id}
    try:
        execute_query(query, params)
    except ClientError as e:
        if e.code == ClientError.Schema.ConstraintValidationFailed:
            # Log the constraint validation error and proceed without raising an exception
            print(f"A tenant with name '{tenant_id}' already exists.")
            pass
        else:
            # If it's a different ClientError, handle it as usual
            print(f"Error creating tenant node for tenant '{tenant_id}': {e}")
            return "Error creating tenant node", 500
    except Exception as e:
        print(f"Error creating tenant node for tenant '{tenant_id}': {e}")
        return "Error creating tenant node", 500

    # Create a user node
    query = """
    MERGE (u:User {diffbotUri: $diffbotUri})
    ON CREATE SET u.firstName = $firstName
    ON CREATE SET u.summary = $summary
    """
    params = {
        "diffbotUri": dbd.data[0].entity.diffbotUri,
        "firstName": dbd.data[0].entity.nameDetail.firstName,
        "summary": dbd.data[0].entity.summary,
    }
    try:
        execute_query(query, params)
    except ClientError as e:
        if e.code == ClientError.Schema.ConstraintValidationFailed:
            # Log the constraint validation error and proceed without raising an exception
            print(f"A user with a diffbotUri '{dbd.data[0].entity.diffbotUri}' already exists.")
            pass
        else:
            # If it's a different ClientError, handle it as usual
            print(f"Error creating user node for tenant '{tenant_id}': {e}")
            return "Error creating user node", 500
    except Exception as e:
        print(f"Error creating user node from diffbot data: {dbd}: {e}")
        return "Error creating user node", 500

    # Create a user-tenant relationship
    query = """
    MATCH (u:User {diffbotUri: $diffbotUri}), (t:Tenant {name: $tenant_id})
    MERGE (u)-[:ATTENDED]->(t)
    """
    params = {
        "diffbotUri": dbd.data[0].entity.diffbotUri,
        "tenant_id": tenant_id,
    }
    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating user-tenant relationship: {e}")
        return "Error creating user-tenant relationship", 500

    # Extract tech from user description
    user_tech = extract_tech(dbd.data[0].entity.description)
    query = """
    UNWIND $user_tech AS tech
    MERGE (t:Tech {name: tech})
    """
    params = {
        "user_tech": user_tech,
    }
    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating user_tech nodes: {e}")
        return "Error creating user_tech nodes", 500
    

    # Create User-Tech relationships
    query = """
    UNWIND $user_tech AS tech
    MATCH (u:User {diffbotUri: $diffbotUri}), (t:TEch {name: tech})
    MERGE (u)-[:KNOWS]->(t)
    """
    params = {
        "diffbotUri": dbd.data[0].entity.diffbotUri,
        "user_tech": user_tech,
    }

    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating user-topic relationships: {e}")
        return "Error creating user-topic relationships", 500

    # Create Employer and Role nodes
    query = """
    UNWIND $employments AS employment
    MERGE (e:Employer {name: employment["employer"]["name"]})
    ON CREATE SET e.description = employment["description"]
    MERGE (r:Role {name: employment["title"]})
    """
    params = {
        "employments": [e.model_dump() for e in dbd.data[0].entity.employments],
    }
    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating employer nodes: {e}")
        return "Error creating employer nodes", 500

    # Create a list of tuples with employer name and tech topics from employer descriptions
    employer_tech = [
        (e.employer.name, extract_tech(e.description))
        for e in dbd.data[0].entity.employments
        if e.employer is not None
    ]

    # Create a list of tuples with title and tech topics from title
    title_tech = [
        (e.title, extract_tech(e.title))
        for e in dbd.data[0].entity.employments
    ]

    # Get all unique tech topics from the employer tech dictionary and title tech
    unique_tech_topics = set()
    for _, tech_list in employer_tech:
        unique_tech_topics.update(tech_list)
    for _, tech_list in title_tech:
        unique_tech_topics.update(tech_list)

    # Create Tech nodes for each unique tech topic
    query = """
    UNWIND $unique_tech_topics AS tech_topic
    MERGE (t:Tech {name: tech_topic})
    """
    params = {
        "unique_tech_topics": list(unique_tech_topics),
    }
    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating tech nodes: {e}")
        return "Error creating tech nodes", 500


    # Create Tech-Employer relationships
    query = """
    UNWIND $employer_tech AS employer_tech
    UNWIND employer_tech[1] AS tech_topic
    MATCH (e:Employer {name: employer_tech[0]}), (t:Tech {name: tech_topic})
    MERGE (e)-[:USES]->(t)
    """
    params = {
        "employer_tech": employer_tech,
    }
    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating tech-employer relationships: {e}")
        return "Error creating tech-employer relationships", 500    


    # Create Tech-Title relationships
    query = """
    UNWIND $title_tech AS title_tech
    UNWIND title_tech[1] AS tech_topic
    MATCH (t:Tech {name: tech_topic}), (r:Role {name: title_tech[0]})
    MERGE (r)-[:USES]->(t)
    """
    params = {
        "title_tech": title_tech,
    }
    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating tech-title relationships: {e}")
        return "Error creating tech-title relationships", 500

    # Create employment relationships
    query = """
    UNWIND $employments AS employment
    MATCH (u:User {diffbotUri: $diffbotUri}), (e:Employer {name: employment["employer"]["name"]})
    MERGE (u)-[:EMPLOYED_AT]->(e)
    """
    params = {
        "diffbotUri": dbd.data[0].entity.diffbotUri,
        "employments": [e.model_dump() for e in dbd.data[0].entity.employments],
    }

    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating employment relationships: {e}")
        return "Error creating employment relationships", 500

    # Create role relationships
    query = """
    UNWIND $employments AS employment
    MATCH (u:User {diffbotUri: $diffbotUri}), (r:Role {name: employment["title"]})
    MERGE (u)-[:WAS]->(r)
    """
    params = {
        "diffbotUri": dbd.data[0].entity.diffbotUri,
        "employments": [e.model_dump() for e in dbd.data[0].entity.employments],
    }

    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating role relationships: {e}")
        return "Error creating role relationships", 500

    response_string = f"Successfully processed {dbd.data[0].entity.nameDetail.firstName}"
    return response_string, 200


# @functions_framework.http
# def import_diffbot(request):

#     # Optional Basic Auth
#     basic_user = os.environ.get("BASIC_AUTH_USER", None)
#     basic_password = os.environ.get("BASIC_AUTH_PASSWORD", None)
#     if basic_user and basic_password:
#         auth_header = request.headers.get("Authorization")
#         if auth_header is None:
#             return "Missing authorization credentials", 401
#         try:
#             request_username, request_password = decode(auth_header)
#             if request_username != basic_user or request_password != basic_password:
#                 return "Unauthorized", 401
#         except Exception as e:
#             logging.error(
#                 f"Problem parsing authorization header: {auth_header}: ERROR: {e}"
#             )
#             return f"Problem with Authorization credentials: {e}", 400

#     payload = request.get_json(silent=True)

#     if payload:
#         try:
#             ddata = DiffBotData(**payload)
#             queries = request.args
#             tenant_id = queries.get("tenant_id", None)
#             if tenant_id is None:
#                 tenant_id = os.environ.get("TENANT_ID", None)
#             return ingest(ddata, tenant_id)
#         except Exception as e:
#             return f"Invalid payload: {e}", 400


# import_diffbot function that processes a JSON list of diffbot data
@functions_framework.http
def import_diffbot_list(request):

    # Optional Basic Auth
    basic_user = os.environ.get("BASIC_AUTH_USER", None)
    basic_password = os.environ.get("BASIC_AUTH_PASSWORD", None)
    if basic_user and basic_password:
        auth_header = request.headers.get("Authorization")
        if auth_header is None:
            return "Missing authorization credentials", 401
        try:
            request_username, request_password = decode(auth_header)
            if request_username != basic_user or request_password != basic_password:
                return "Unauthorized", 401
        except Exception as e:
            logging.error(
                f"Problem parsing authorization header: {auth_header}: ERROR: {e}"
            )
            return f"Problem with Authorization credentials: {e}", 400

    payload = request.get_json(silent=True)

    # Check payload is a JSON list of dictionaries
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        return "Invalid payload: expected a list of dictionaries", 400
    
    # Process each item in the list
    results = []
    for item in payload:
        try:
            ddata = DiffBotData(**item)
            queries = request.args
            tenant_id = queries.get("tenant_id", None)
            if tenant_id is None:
                tenant_id = os.environ.get("TENANT_ID", None)
            results.append(ingest(ddata, tenant_id))
        except Exception as e:
            results.append(f"Invalid payload: {e}")
    
    return results, 200
