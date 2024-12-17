from os.path import dirname

# from trilogy_public_models import models
from sys import path

import trilogy_public_models.bigquery.thelook_ecommerce as model

path.append(dirname(dirname(__file__)))
from trilogy import parse

from trilogyt.graph import process_raw

QUERIES = [
    """
           
auto users.name <- concat(users.first_name,' ',users.last_name);

select users.name, orders.id.count
order by orders.id.count desc
limit 100;

select users.name, orders.id.count
order by orders.id.count desc
limit 100;
           

auto san_fran_order<- filter orders.id where users.city = 'San Francisco';
           
select users.name, san_fran_order.count
limit 100;


select users.last_name, orders.id.count, users.state
order by orders.id.count desc;

           """
]

queries = []

if __name__ == "__main__":
    for q in QUERIES:
        _, new_queries = parse(q, environment=model)
        queries += new_queries

    for q in queries:
        print(type(q))
    kv = process_raw(queries, model)
    for k, v in kv.items():
        print(k)
        print(v)
