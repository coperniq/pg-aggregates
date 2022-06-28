import type { Plugin } from "graphile-build";
import type {
  PgAttribute,
  QueryBuilder,
  PgProc,
  PgClass,
  SQL,
} from "graphile-build-pg";
import type { GraphQLResolveInfo, GraphQLFieldConfigMap } from "graphql";
import { AggregateSpec } from "./interfaces";
import { OrderSpec } from "./OrderByAggregatesPlugin";

const AddAggregateTypesPlugin: Plugin = (builder) => {
  // Create the aggregates type for each table
  builder.hook("init", (init, build, _context) => {
    const {
      newWithHooks,
      graphql: {
        GraphQLObjectType,
        GraphQLList
      },
      inflection,
      pgIntrospectionResultsByKind,
      pgOmit: omit,
      getTypeByName,
    } = build;

    pgIntrospectionResultsByKind.class.forEach((table: PgClass) => {
      if (!table.namespace) {
        return;
      }
      if (omit(table, "read")) {
        return;
      }
      if (table.tags.enum) {
        return;
      }
      if (!table.isSelectable) {
        return;
      }

      const GrapqhQLJSON = getTypeByName('JSON');
      if (!GrapqhQLJSON) {
        throw new Error('JSON scalar type is missing');
      }

      /* const AggregateContainerType = */
      newWithHooks(
        GraphQLObjectType,
        {
          name: inflection.aggregateContainerType(table),
          fields: {
            keys: {
              type: new GraphQLList(GrapqhQLJSON),
              resolver(parent: any) {
                return parent.keys || [];
              },
            },
          },
        },
        {
          isPgAggregateContainerType: true,
          pgIntrospection: table,
        },
        true
      );
    });

    return init;
  });

  // Hook the '*Aggregates' type for each table to add the "sum" operation
  builder.hook(
    "GraphQLObjectType:fields",
    function addAggregateFieldsToAggregateType(fields, build, context) {
      const {
        pgField,
        inflection,
        newWithHooks,
        graphql: { GraphQLObjectType },
        getSafeAliasFromResolveInfo,
      } = build;
      const {
        fieldWithHooks,
        scope: { isPgAggregateContainerType, pgIntrospection: table },
      } = context;
      if (!isPgAggregateContainerType) {
        return fields;
      }

      return build.extend(
        fields,
        (build.pgAggregateSpecs as AggregateSpec[]).reduce(
          (memo: GraphQLFieldConfigMap<unknown, unknown>, spec) => {
            const AggregateType = newWithHooks(
              GraphQLObjectType,
              {
                name: inflection.aggregateType(table, spec),
              },
              {
                isPgAggregateType: true,
                pgAggregateSpec: spec,
                pgIntrospection: table,
              },
              true
            );

            if (!AggregateType) {
              // No aggregates for this connection for this spec, abort
              return memo;
            }
            const fieldName = inflection.aggregatesField(spec);
            return build.extend(memo, {
              ...fields,
              [fieldName]: pgField(
                build,
                fieldWithHooks,
                fieldName,
                {
                  description: `${spec.HumanLabel} aggregates across the matching connection (ignoring before/after/first/last/offset)`,
                  type: AggregateType,
                  resolve(
                    parent: any,
                    _args: any,
                    _context: any,
                    resolveInfo: GraphQLResolveInfo
                  ) {
                    const safeAlias = getSafeAliasFromResolveInfo(resolveInfo);
                    return parent[safeAlias];
                  },
                },
                {
                  isPgAggregateField: true,
                  pgAggregateSpec: spec,
                  pgFieldIntrospection: table,
                } // scope,
              ),
            });
          },
          {}
        )
      );
    }
  );

  // Hook the sum aggregates type to add fields for each numeric table column
  builder.hook("GraphQLObjectType:fields", (fields, build, context) => {
    const {
      pgSql: sql,
      graphql: { GraphQLNonNull, GraphQLList },
      inflection,
      getSafeAliasFromAlias,
      getSafeAliasFromResolveInfo,
      pgField,
      pgIntrospectionResultsByKind,
      pgGetComputedColumnDetails: getComputedColumnDetails,
    } = build;
    const {
      fieldWithHooks,
      scope: {
        isPgAggregateType,
        pgIntrospection: table,
        pgAggregateSpec: spec,
      },
    } = context;
    if (!isPgAggregateType || !table || table.kind !== "class" || !spec) {
      return fields;
    }

    return {
      ...fields,
      // Figure out the columns that we're allowed to do a `SUM(...)` of
      ...table.attributes.reduce(
        (memo: GraphQLFieldConfigMap<any, any>, attr: PgAttribute) => {
          if (
            (spec.shouldApplyToEntity && !spec.shouldApplyToEntity(attr)) ||
            !spec.isSuitableType(attr.type)
          ) {
            return memo;
          }
          const [pgType, pgTypeModifier] = spec.pgTypeAndModifierModifier
            ? spec.pgTypeAndModifierModifier(attr.type, attr.typeModifier)
            : [attr.type, attr.typeModifier];
          const Type = build.pgGetGqlTypeByTypeIdAndModifier(
            pgType.id,
            pgTypeModifier
          );
          if (!Type) {
            return memo;
          }
          const fieldName = inflection.column(attr);

          // find ordering type for field's table, if any
          const tableTypeName = inflection.tableType(table);
          const TableOrderByType = build.getTypeByName(
            inflection.orderByType(tableTypeName)
          );

          return build.extend(memo, {
            [fieldName]: pgField(
              build,
              fieldWithHooks,
              fieldName,
              ({ addDataGenerator }: any) => {
                addDataGenerator((parsedResolveInfoFragment: any) => {
                  return {
                    pgQuery: (queryBuilder: QueryBuilder) => {
                      const args = parsedResolveInfoFragment.args;
                      // add ordering (if provided)
                      // slightly modified copy-paste of https://github.com/graphile/graphile-engine/blob/v4/packages/graphile-build-pg/src/plugins/PgConnectionArgOrderBy.js#L144-L171
                      const orders: Array<[SQL, boolean, boolean | undefined]> = [];
                      if (args.orderBy?.length) {
                        args.orderBy.forEach((item: { specs: OrderSpec | Array<OrderSpec>, unique: boolean }) => {
                          const { specs } = item;
                          (Array.isArray(specs[0]) || specs.length === 0 ? specs as Array<OrderSpec> : [specs as OrderSpec])
                            .forEach(([col, ascending, specNullsFirst]) => {
                              const expr = typeof col === "string"
                                ? sql.fragment`${queryBuilder.getTableAlias()}.${sql.identifier(
                                  col as string
                                )}`
                                : typeof col === "function"
                                  ? (col as ((options: { queryBuilder: QueryBuilder }) => SQL))({ queryBuilder })
                                  : col as SQL;
                              // If the enum specifies null ordering, use that
                              // Otherwise, use the orderByNullsLast option if present
                              const nullsFirst =
                                specNullsFirst != null
                                  ? specNullsFirst
                                  : undefined;
                              orders.push([expr, ascending, nullsFirst]);
                            });
                        });
                      }
                      const orderExpr = orders.length ? sql.fragment`ORDER BY ${sql.join(
                        orders
                          .map(([orderSql, asc, nullsFirst]) =>
                            sql.fragment`${orderSql} ${
                              asc ?
                                sql.fragment`ASC` :
                                sql.fragment`DESC`
                            } ${
                              nullsFirst === true
                                ? sql.fragment`NULLS FIRST`
                                : nullsFirst === false
                                  ? sql.fragment`NULLS LAST`
                                  : null
                            }`
                          ),
                        ", "
                      )}` : "";
                      // Note this expression is just an sql fragment, so you
                      // could add CASE statements, function calls, or whatever
                      // you need here
                      const sqlColumn = sql.fragment`${queryBuilder.getTableAlias()}.${sql.identifier(
                        attr.name
                      )} ${orderExpr}`;
                      const sqlAggregate = spec.sqlAggregateWrap(sqlColumn);
                      queryBuilder.select(
                        sqlAggregate,
                        // We need a unique alias that we can later reference in the resolver
                        getSafeAliasFromAlias(parsedResolveInfoFragment.alias)
                      );
                    },
                  };
                });
                return {
                  description: `${spec.HumanLabel} of ${fieldName} across the matching connection`,
                  type: spec.isNonNull ? new GraphQLNonNull(Type) : Type,
                  args: {
                    ...(TableOrderByType ? {
                      orderBy: {
                        type: new GraphQLList(new GraphQLNonNull(TableOrderByType)),
                        description: build.wrapDescription(
                          `The method to use when ordering \`${tableTypeName}\` for aggregate ${spec.HumanLabel} (if sensible).`,
                          "arg"
                        ),
                      }
                    } : null)
                  },
                  resolve(
                    parent: any,
                    _args: any,
                    _context: any,
                    resolveInfo: GraphQLResolveInfo
                  ) {
                    const safeAlias = getSafeAliasFromResolveInfo(resolveInfo);
                    return parent[safeAlias];
                  },
                };
              },
              {
                // In case anyone wants to hook us, describe ourselves
                isPgConnectionAggregateField: true,
                pgFieldIntrospection: attr,
              },
              false,
              {
                pgType,
                pgTypeModifier,
              }
            ),
          });
        },
        {}
      ),
      ...pgIntrospectionResultsByKind.procedure.reduce(
        (memo: GraphQLFieldConfigMap<any, any>, proc: PgProc) => {
          if (proc.returnsSet) {
            return memo;
          }
          const type = pgIntrospectionResultsByKind.typeById[proc.returnTypeId];
          if (
            (spec.shouldApplyToEntity && !spec.shouldApplyToEntity(proc)) ||
            !spec.isSuitableType(type)
          ) {
            return memo;
          }
          const computedColumnDetails = getComputedColumnDetails(
            build,
            table,
            proc
          );
          if (!computedColumnDetails) {
            return memo;
          }
          const { pseudoColumnName } = computedColumnDetails;
          const fieldName = inflection.computedColumn(
            pseudoColumnName,
            proc,
            table
          );
          return build.extend(memo, {
            [fieldName]: build.pgMakeProcField(fieldName, proc, build, {
              fieldWithHooks,
              computed: true,
              aggregateWrapper: spec.sqlAggregateWrap,
              pgTypeAndModifierModifier: spec.pgTypeAndModifierModifier,
              description: `${
                spec.HumanLabel
              } of this field across the matching connection.${
                proc.description ? `\n\n---\n\n${proc.description}` : ""
              }`,
            }),
          });
        },
        {}
      ),
    };
  });
};

export default AddAggregateTypesPlugin;
