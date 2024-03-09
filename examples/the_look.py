import trilogy_public_models.bigquery.thelook_ecommerce as model

# from trilogy_public_models import models
from sys import path
from os.path import dirname
path.append(dirname(dirname(__file__)))
from preqlt.graph import process, process_raw
from preql import parse

QUERIES = ["""
           
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

           """]

queries = []

if __name__ == "__main__":
    for q in QUERIES:
        _, new_queries = parse(q, environment=model)
        queries += new_queries

    for q in queries:
        print(type(q))
    kv = process_raw(queries, model)
    for k,v in kv.items():
        print(k)
        print(v)