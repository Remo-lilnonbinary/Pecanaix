import os

import chromadb

CHROMA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "chroma_db")


def get_client():
    os.makedirs(CHROMA_PATH, exist_ok=True)
    return chromadb.PersistentClient(path=CHROMA_PATH)


def get_alumni_collection():
    client = get_client()
    return client.get_or_create_collection(
        name="alumni_profiles",
        metadata={"description": "Embedded alumni profiles for semantic matching"},
    )


def embed_alumni(alumni_list):
    collection = get_alumni_collection()
    documents = []
    ids = []
    metadatas = []

    for a in alumni_list:
        profile_text = (
            f"{a.get('name', '')} graduated in {a.get('graduation_year', '')} "
            f"with a {a.get('degree', '')} from the {a.get('department', '')} department. "
            f"Currently working as {a.get('job_title', '')} at {a.get('company', '')} "
            f"in the {a.get('industry', '')} industry. "
            f"Based in {a.get('location_city', '')}, {a.get('location_country', 'UK')}. "
            f"Interests: {a.get('interests', '')}. "
            f"Past events attended: {a.get('past_events', 'none')}. "
            f"Engagement score: {a.get('engagement_score', 50)}/100."
        )
        documents.append(profile_text)
        ids.append(str(a["id"]))
        metadatas.append(
            {
                "alumni_id": a["id"],
                "location_city": str(a.get("location_city", "")),
                "industry": str(a.get("industry", "")),
                "graduation_year": int(a.get("graduation_year", 0)),
                "engagement_score": int(a.get("engagement_score", 50)),
            }
        )

    collection.upsert(documents=documents, ids=ids, metadatas=metadatas)
    print(f"Embedded {len(documents)} alumni profiles into vector store.")
    return len(documents)


def search_alumni(event_description, n_results=60, where_filter=None):
    collection = get_alumni_collection()
    query_params = {
        "query_texts": [event_description],
        "n_results": min(n_results, collection.count()),
    }
    if where_filter:
        query_params["where"] = where_filter

    results = collection.query(**query_params)
    matches = []
    if results and results["ids"] and results["ids"][0]:
        for i, alumni_id in enumerate(results["ids"][0]):
            matches.append(
                {
                    "alumni_id": int(alumni_id),
                    "similarity_distance": results["distances"][0][i] if results["distances"] else None,
                    "profile_text": results["documents"][0][i] if results["documents"] else "",
                    "metadata": results["metadatas"][0][i] if results["metadatas"] else {},
                }
            )
    return matches


if __name__ == "__main__":
    import sys

    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from tools.database import get_all_alumni

    alumni = get_all_alumni()
    embed_alumni(alumni)
    print("\nTesting search: 'Fintech careers panel in London for recent finance graduates'")
    results = search_alumni(
        "Fintech careers panel in London for recent finance graduates",
        n_results=5,
    )
    for r in results:
        print(f"  Alumni #{r['alumni_id']} (distance: {r['similarity_distance']:.3f})")
        print(f"    {r['profile_text'][:120]}...")
        print()
