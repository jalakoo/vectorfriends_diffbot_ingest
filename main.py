from basicauth import decode
from typing import List, Optional
from neo4j import GraphDatabase, basic_auth
from pydantic import BaseModel, Field
import functions_framework
import os
import logging

# Note: Pydantic does not appear to work properly in Google Cloud Functions

HOST = os.environ.get("NEO4J_URI")
PASSWORD = os.environ.get("NEO4J_PASSWORD")
USER = os.environ.get("NEO4J_USER", "neo4j")
DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")


class Location(BaseModel):
    isCurrent: Optional[bool] = None
    address: Optional[str] = None


class DiffBotTime(BaseModel):
    timestamp: Optional[int] = None


class Employment(BaseModel):
    isCurrent: bool
    employer: Optional[dict] = None
    description: Optional[str] = None
    location: Optional[Location] = None
    title: Optional[str] = None
    to: Optional[DiffBotTime] = None


class DiffBotName(BaseModel):
    firstName: Optional[str] = None
    lastName: Optional[str] = None


class Entity(BaseModel):
    diffbotUri: Optional[str] = None
    nameDetail: Optional[DiffBotName] = None
    origins: List[str] = Field(default_factory=list)
    summary: Optional[str] = None
    languages: List[dict] = Field(default_factory=list)
    employments: List[Employment] = Field(default_factory=list)

    def get_languages(self) -> List[str]:
        return [lang["normalizedValue"] for lang in self.languages]


class Data(BaseModel):
    entity: Entity


class DiffBotData(BaseModel):
    data: List[Data] = Field(default_factory=list)


def execute_query(query, params):
    try:
        with GraphDatabase.driver(
            HOST, auth=basic_auth(USER, PASSWORD), database=DATABASE
        ) as driver:
            return driver.execute_query(query, params)
    except Exception as e:
        print(f"Upload query error: {e}")
        return None


def ingest(dbd: DiffBotData, tenant_id: str):

    # Create new tenant node if it doesn't already exist
    query = """
    MERGE (t:Tenant {name: $tenant_id})
    ON CREATE SET t.created_at = datetime()
    """
    params = {"tenant_id": tenant_id}
    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating tenant node: {e}")
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
    except Exception as e:
        print(f"Error creating user node: {e}")
        return "Error creating user node", 500

    # Create a user-tenant relationship
    query = """
    MATCH (u:User {diffbotUri: $diffbotUri})
    MERGE (u)-[:ATTENDED]->(t:Tenant {name: $tenant_id})
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

    # Create employer nodes
    query = """
    UNWIND $employments AS employment
    MERGE (e:Employer {name: employment["employer"]["name"]})
    ON CREATE SET e.description = employment["description"]
    """
    params = {
        "employments": [e.model_dump() for e in dbd.data[0].entity.employments],
    }
    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating employer nodes: {e}")
        return "Error creating employer nodes", 500

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

    print(f"employment params: {params}")

    try:
        execute_query(query, params)
    except Exception as e:
        print(f"Error creating employment relationships: {e}")
        return "Error creating employment relationships", 500

    return "OK", 200


@functions_framework.http
def import_diffbot(request):

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

    if payload:
        try:
            ddata = DiffBotData(**payload)
            queries = request.args
            tenant_id = queries.get("tenant_id", None)
            if tenant_id is None:
                tenant_id = os.environ.get("TENANT_ID", None)
            return ingest(ddata, tenant_id)
        except Exception as e:
            return f"Invalid payload: {e}", 400
