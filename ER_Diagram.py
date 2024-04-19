# Databricks notebook source
                    +---------------------+
                    |   AverageCost       |
                    +---------------------+
                    | fscldt_id (PK)      |
                    | sku_id (FK)         |
                    | average_unit_standardcost |
                    | average_unit_landedcost   |
                    +---------------------+
                               |
                               |
                      +-------------------+
                      |   Transaction     |
                      +-------------------+
                      | order_id (PK)     |
                      | line_id (PK)      |
                      | type              |
                      | dt (FK)           |
                      | pos_site_id (FK)  |
                      | sku_id (FK)       |
                      | price_substate_id (FK) |
                      | sales_units       |
                      | sales_dollars     |
                      | discount_dollars  |
                      | original_order_id |
                      | original_line_id  |
                      +-------------------+
                               |
                               |
                      +-------------------+
                      |   Clnd            |
                      +-------------------+
                      | fscldt_id (PK)    |
                      | fscldt_label      |
                      | fsclwk_id         |
                      | fsclwk_label      |
                      | fsclmth_id        |
                      | fsclmth_label     |
                      | fsclqrtr_id       |
                      | fsclqrtr_label    |
                      | fsclyr_id         |
                      | fsclyr_label      |
                      | ssn_id            |
                      | ssn_label         |
                      | ly_fscldt_id      |
                      | lly_fscldt_id     |
                      | fscldow           |
                      | fscldom           |
                      | fscldoq           |
                      | fscldoy           |
                      | fsclwoy           |
                      | fsclmoy           |
                      | fsclqoy           |
                      | date              |
                      +-------------------+
                               |
                               |
                      +-------------------+
                      |   Hldy            |
                      +-------------------+
                      | hldy_id (PK)      |
                      | hldy_label        |
                      +-------------------+
        +---------------------------------------------+
        |                       |                     |
+-------------------+  +-------------------+  +-------------------+
|   InvLoc          |  |   InvStatus       |  |   PosSite         |
+-------------------+  +-------------------+  +-------------------+
| loc               |  | code_id (PK)      |  | site_id (PK)      |
| loc_label         |  | code_label        |  | site_label        |
| loctype           |  | bckt_id           |  | subchnl_id        |
| loctype_label     |  | bckt_label        |  | subchnl_label     |
|                   |  | ownrshp_id         | | chnl_id           |
|                   |  | ownrshp_label      | | chnl_label        |
+-------------------+  +-------------------+  +-------------------+
                               |
                      +-------------------+
                      |   PriceState      |
                      +-------------------+
                      | substate_id (PK)  |
                      | substate_label    |
                      | state_id          |
                      | state_label       |
                      +-------------------+
                               |
                      +-------------------+
                      |   Prod            |
                      +-------------------+
                      | sku_id (PK)       |
                      | sku_label         |
                      | stylclr_id        |
                      | stylclr_label     |
                      | styl_id           |
                      | styl_label        |
                      | subcat_id         |
                      | subcat_label      |
                      | cat_id            |
                      | cat_label         |
                      | dept_id           |
                      | dept_label        |
                      | issvc             |
                      | isasmbly          |
                      | isnfs             |
                      +-------------------+
                               |
                      +-------------------+
                      |   RtLoc           |
                      +-------------------+
                      | str               |
                      | str_label         |
                      | dstr              |
                      | dstr_label        |
                      | rgn               |
                      | rgn_label         |
                      +-------------------+

