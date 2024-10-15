Basics
========

General setup reminder
-----------------------

**The naming convention : a pillar for your refinery**

First and foremost, users of the refinery need to gather and define together the naming convention. 
This is the convention of naming of all the series (primary and formulas) available in the data referential.
Every person of the team needs to know it perfectly. He/She will define new data following this convention and find quickly and efficiently the data he/she needs.


Here are some tips :

* Find the separator of your keywords. Let's choose a dot for our example "."
* Gather all the types of keywords you could need. Let's assume we work with fruits transactions, here is our list :

    * type of aliment
    * type of fruit
    * country of origin
    * country of selling
    * type of transaction (sell/buy)
    * unit (currency/weight)
    * farmer
    * buyer
    * granularity

* Choose the most intuitive order of keywords : **aliment.fruit.transaction.farmer.buyer.from_country.to_country.unit.granularity**

example : *fruit.banana.buy.misterfield.superfrenchshop.civ.fr.eur/t.d*


How to create / update / read "primary" time series
---------------------------------------------------



Going further: playing with revisions
-------------------------------------

Adding and fetching metadata
----------------------------

Using find with metadata
------------------------

Using formulas : create a formula
---------------------------------

Similarities and differences with primary series
------------------------------------------------

Using the formula editor
------------------------

Seeing all the formulas
-----------------------

