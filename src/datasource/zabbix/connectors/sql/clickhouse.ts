function historyQuery(itemids, table, timeFrom, timeTill, intervalSec, aggFunction) {
  const timeExpression = `toDateTime(intDiv(clock, ${intervalSec}) * ${intervalSec})`;
  return `
      SELECT toString(itemid) AS metric, ${timeExpression} AS time, ${aggFunction}(value) AS value
      FROM (
        SELECT itemid, clock, ns, argMax(value, _version) AS value
        FROM ${table}
        WHERE itemid IN (${itemids})
          AND clock
            > ${timeFrom}
          AND clock
            < ${timeTill}
        GROUP BY itemid, clock, ns
      ) AS history_dedup
      GROUP BY metric, time
      ORDER BY time ASC
  `;
}

function trendsQuery(itemids, table, timeFrom, timeTill, intervalSec, aggFunction, valueColumn) {
  const timeExpression = `toDateTime(intDiv(clock, ${intervalSec}) * ${intervalSec})`;
  return `
      SELECT toString(itemid) AS metric, ${timeExpression} AS time, ${aggFunction}(${valueColumn}) AS value
      FROM (
        SELECT
          itemid,
          clock,
          argMax(num, _version) AS num,
          argMax(value_min, _version) AS value_min,
          argMax(value_avg, _version) AS value_avg,
          argMax(value_max, _version) AS value_max
        FROM ${table}
        WHERE itemid IN (${itemids})
          AND clock
            > ${timeFrom}
          AND clock
            < ${timeTill}
        GROUP BY itemid, clock
      ) AS trends_dedup
      GROUP BY metric, time
      ORDER BY time ASC
  `;
}

function testQuery() {
  return `
      SELECT toString(itemid) AS metric, toDateTime(clock) AS time, value_avg AS value
      FROM trends_uint
      LIMIT 1
  `;
}

const clickhouse = {
  historyQuery,
  trendsQuery,
  testQuery,
};

export default clickhouse;
