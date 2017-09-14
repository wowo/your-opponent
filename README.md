## Queries

### Get groups
    MATCH (n {name: 'Sznapka'})-[r]->()
    WITH r.year as y, r.round as r, r.group as g, COLLECT(DISTINCT (r.year + " - " + r.round + " - " + r.group)) as pos
    RETURN y, r, g
    ORDER BY y desc, r desc


### Get player's matches
    MATCH (n {name: 'Sznapka'})-[r]->(m)
    RETURN n, r, m
    ORDER BY r.year DESC, r.round DESC