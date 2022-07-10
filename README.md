# Crud No More

## Introduction

Crud No More is a library for synchronizing data between a [Postgres database](postgresql.org) and other components in a distributed system, for example a SPA (Single Page App).

In its current state it can be used to speed up the development of CRUD endpoints of a [Node.js](https://nodejs.org/en/) backend.

An important concept in CrudNoMore is the _document_.
A document is a [Directed Acyclic Graph (DAG)](https://en.wikipedia.org/wiki/Directed_acyclic_graph) of database tables with a single root table.
From the root table, all other tables can be reached by traversing foreign key constraints.
The primary key of the root table identifies instances of the document.
Documents can be declared in terms of table columns and the relationships between table columns.

| ![space-1.jpg](/doc/assets/document.png) |
| :--------------------------------------: |

| **Example document.**

## Workflow

1. Export database schema as typescript types and variables using the cli
1. Define your document schemas in terms of the code generated in the previous step.
1. Implement your endpoints using the document API.

## Examples
