import uuid
from fastmcp import FastMCP, Context
from fastmcp.server.openapi import RouteMap, RouteType
from duckduckgo_search import DDGS
import logging
import os
import yaml
import asyncio
from pydantic import BaseModel, Field
from typing import Dict, List, Any
from neo4j import Query, GraphDatabase, RoutingControl, Result
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(".env")
client_secret=os.getenv('CLIENT_SECRET')
client_id=os.getenv('CLIENT_ID')
database_uri=os.getenv('DB_URI')
database_user=os.getenv('DB_USER')
database_password=os.getenv('DB_PASSWORD')
database_name=os.getenv('DB_NAME')
session_database_name=os.getenv('SESSION_DB_NAME')
logger.info(f"database_uri: {database_uri}")

driver = GraphDatabase.driver(
    database_uri,
    auth=(database_user, database_password)
)
driver.verify_connectivity(database=database_name)


# Reuqest and Response Models (Pydantic)
class AccountRequest(BaseModel):
    accountNumber: str = Field(..., description = "The Account Number to investigate")

class CypherRequest(BaseModel):
    query: str = Field(..., description = "The Cypher query to execute")
    parameters: dict = Field(default_factory=dict, description = "Parameters for the Cypher query")

class UserInfo (BaseModel):
    name: str = Field(..., description = "The name of the user")

class Note (BaseModel):
    content: str =Field(..., description = "The content of the note")
    


mcp = FastMCP("paysim-mcp")

# Utility function to run Cypher queries in a managed transaction
def run_cypher_( 
            query: str, 
            parameters: dict = {}, 
            transformer = lambda result: [record.data() for record in result],
            routing: RoutingControl = RoutingControl.READ,
            database: str = database_name
        ) -> List[Dict[str, Any]]:
        return driver.execute_query(
            routing_= routing,
            query_= query,
            parameters_= parameters,
            database_= database,
            result_transformer_= transformer
        ) 

@mcp.tool(name="get_schema", description="Get the schema of the graph database")
def get_schema() -> list:
    '''Retrieve the schema of the graph database'''
    return run_cypher_(
        '''//cypher
            CALL apoc.meta.data()
            YIELD label, property, type, other, elementType
            RETURN label, property, type, other, elementType
        ''',
        None, 
        transformer=lambda result: [record.data() for record in result]
    )

@mcp.tool(name="run_cypher", description="Run a Cypher query against the graph database")
def run_cypher(query: str, parameters: dict = {}):
    '''Execute a Cypher query against the graph database with optional parameters in a managed read transaction.'''
    return run_cypher_(query, parameters, transformer=lambda result: [record.data() for record in result])


@mcp.tool()
async def where_is_the_money_coming_from(request: AccountRequest, ctx: Context):
    """
    Find where the money is coming from given an account
    
    Args:
        request: AccountRequest object containing the account number
    
    Returns:
        List containing distnace from the account (number of hops away), 
        number of transactions, and distinct accounts sending money
    """
    logger.log(logging.INFO, f"Session ID: {ctx.session_id} Client ID: {ctx.client_id}")
    return run_cypher_(
        '''//cypher
            match p=(c:Account)(
                ()-[:PERFORM]->(tx1:Transaction)-[:BENEFITS_TO]->()
            ){1,3}(c2:Account{accountNumber: $accountNumber})
            return 
                length(p)/2 as number_of_hops_away,
                sum(count{ unwind [x in nodes(p) where x:Transaction] as tx return distinct tx  }) as number_of_transactions,
                count(distinct c) as distinct_accounts
        ''',
        parameters={"accountNumber": request.accountNumber}, #"ACC_MULE_129"
    )

@mcp.tool()
async def customer_overview(request: AccountRequest, ctx: Context):
    """
    Find customer information and account overview given an account number
    
    Args:
        request: AccountRequest object containing the account number
    
    Returns:
        Dictionary containing customer information and list of accounts
    """
    logger.log(logging.INFO, f"Session ID: {ctx.session_id} Client ID: {ctx.client_id}")
    result = run_cypher_(
        '''//cypher
            match (a:Account{accountNumber: $accountNumber})
            with a,     
                collect { 
                    match (c:Customer)-[:HAS_ACCOUNT]->(a)
                    with c, 
                    { 
                        customer: c{.*},
                        account_list: collect 
                        {
                            match (c)-[:HAS_ACCOUNT]->(a2)
                            with a2, {
                                account_number: a2.accountNumber,
                                total_sent: collect{ match (a2)-[:PERFORM]->(tx) return sum(tx.amount) }[0],
                                total_received: collect{ match (tx)-[:BENEFITS_TO]->(a2) return sum(tx.amount) }[0],
                                number_of_transactions: count { (a2)-[:PERFORM|BENEFITS_TO]-(tx) }
                            } as account_list
                            return account_list
                        } 
                    } as customer_data
                    return customer_data
                }[0] as customer_info
            return customer_info                      
        ''',
        parameters={"accountNumber": request.accountNumber}, #"ACC_MULE_129"
        transformer=lambda result: result.single().data()
    )
    return result

# As we have not added authentication to this server,
# we will simply ask for the username   
@mcp.tool
async def save_note(note:Note, ctx: Context) -> str:
    """Save a note to the database"""
    result = await ctx.elicit(
        message="Please provide your user name",
        response_type=UserInfo
    )
    
    if result.action == "accept":
        user = result.data
    elif result.action == "decline":
        return "Information not provided"
    else:  # cancel
        return "Operation cancelled"
        
    run_cypher_(
        '''//cypher
            merge (s:Session {sessionId: $sessionId})
            merge (u:User {name: $user})
            merge (n:Note {noteId: $noteId})
                set n.content = $note,
                    n.createdAt = datetime()
            merge (s)-[:HAS_NOTE]->(n)
            merge (u)-[:HAS_SESSION]->(s)
        ''',
        parameters = { 
            "sessionId": ctx.session_id,
            "user" : user.name, 
            "note" : note.content, 
            "noteId": uuid.uuid4().hex
        },
        routing = RoutingControl.WRITE,
        database = session_database_name,
        transformer=lambda result: [record.data() for record in result]
    )
    return f"Note saved for {user.name}, with Session ID {ctx.session_id}."


@mcp.tool()
def duckduckgo_search(query: str, max_results: int = 5) -> List[Dict[str, Any]]:
    """
    Search the web using DuckDuckGo
    
    Args:
        query: Search query string
        max_results: Maximum number of results to return (default: 5)
    
    Returns:
        List of search results with title, body, and href
    """
    try:
        with DDGS() as ddgs:
            results = []
            for result in ddgs.text(query, max_results=max_results):
                results.append({
                    "title": result.get("title", ""),
                    "body": result.get("body", ""),
                    "url": result.get("href", ""),
                    "source": "DuckDuckGo"
                })
            return results
    except Exception as e:
        return [{"error": f"DuckDuckGo search failed: {str(e)}"}]

# Run the MCP server
if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="127.0.0.1",
        port=9345,
        path="/",
        log_level="debug",
    )