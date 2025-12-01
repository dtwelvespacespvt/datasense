import { useState } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@catalyst/table";
import { CustomTooltip } from "./Tooltip";
import {
  ArrowsPointingInIcon,
  ArrowsPointingOutIcon,
  MinusIcon,
  ArrowDownTrayIcon,
  TableCellsIcon,
} from "@heroicons/react/24/outline";
import Minimizer from "../Minimizer/Minimizer";
import { useExportData } from "@/hooks/messages";
import { ISQLQueryStringResult } from "./types";
import { CodeBlock } from "../Conversation/CodeBlock";

// TODO: Remove after defining this better on backend
export const DynamicTable: React.FC<{
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: { columns: string[]; rows: any[][] };
  initialCreatedAt?: Date;
  minimize?: boolean;
  linked_id?: string;
  messageId?: string;
  updateLinkedSQLRunResult?: (sql_string_result_id: string, content: string) => void;
  onSaveSQLStringResult?: (
    sql_string_result_id: string
  ) => (data?: { created_at: string; chartjs_json: string } | void) => void;
  linkedSqlQuery?: ISQLQueryStringResult | null;
}> = ({ data, minimize, linked_id, linkedSqlQuery, messageId, updateLinkedSQLRunResult, onSaveSQLStringResult }) => {
  const [minimized, setMinimized] = useState(minimize || false);
  const [limitedView, setLimitedView] = useState(true);
  const [showSql, setShowSql] = useState(false);
  const { mutate: exportData } = useExportData();
  const [currentSQL, setCurrentSQL] = useState(linkedSqlQuery?.content.sql || "");

  const handleExpand = () => {
    if (minimized) setMinimized(false);
    else if (limitedView) setLimitedView(false);
  };

  return (
    <Minimizer
      minimized={minimized}
      setMinimized={setMinimized}
      label="Data results"
      classes="bg-gray-800"
    >
      <div className="relative">
        <div className="absolute  top-0 right-0 m-2 flex gap-1 z-10">
          <CustomTooltip hoverText={`${!showSql ? "Show SQL" : "Hide SQL"}`}>
            <button
              tabIndex={-1}
              onClick={() => setShowSql(!showSql)}
              className="p-1"
            >
              <TableCellsIcon className="w-6 h-6 [&>path]:stroke-[2]" />
            </button>
          </CustomTooltip>
        </div>
      </div>
      {showSql &&
        linkedSqlQuery &&
        onSaveSQLStringResult &&
        updateLinkedSQLRunResult ? (
        <CodeBlock
          key={`message-${messageId}-code-${linkedSqlQuery.result_id}`}
          dialect={linkedSqlQuery.content.dialect}
          code={currentSQL}
          resultId={linkedSqlQuery.result_id}
          onUpdateSQLRunResult={(id, content) => {
            updateLinkedSQLRunResult(id, content);
            setShowSql(false);
          }}
          onSaveSQLStringResult={onSaveSQLStringResult(
            linkedSqlQuery.result_id
          )}
          forChart={linkedSqlQuery.content.for_chart}
          onSQLChange={setCurrentSQL}
        />
      ) : (
        <div className="relative">
          <Table
            grid
            bleed
            striped
            dense
            maxRows={limitedView ? 5 : data.rows.length + 10}
            className="ml-0 mr-0 [--gutter:theme(spacing.6)]"
          >
            <TableHead>
              <TableRow>
                {data.columns.map((header: string, index: number) => (
                  <TableHeader key={index}>{header}</TableHeader>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {data.rows.map((row: string[] | number[], index: number) => {
                return (
                  <TableRow key={index}>
                    {row.map((item: string | number, cellIndex: number) => (
                      <TableCell key={cellIndex} className="font-medium pl-8">
                        {item}
                      </TableCell>
                    ))}
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>

          <div className="absolute bottom-0 right-0 m-2 flex gap-1">
            {/* Minimize Icon */}
            <CustomTooltip hoverText="Minimize">
              {/* On minimize, also collapse if already expanded */}
              <button
                tabIndex={-1}
                onClick={() => {
                  setMinimized(true);
                  setLimitedView(true);
                }}
                className="p-1"
              >
                <MinusIcon className="w-6 h-6 [&>path]:stroke-[2]" />
              </button>
            </CustomTooltip>
            {data.rows.length > 4 && // 4 data rows + 1 header (columns) row
              (limitedView ? (
                /* Expand Icon */
                <CustomTooltip hoverText="Expand">
                  <button tabIndex={-1} onClick={handleExpand} className="p-1">
                    <ArrowsPointingOutIcon className="w-6 h-6 [&>path]:stroke-[2]" />
                  </button>
                </CustomTooltip>
              ) : (
                /* Contract Icon */
                <CustomTooltip hoverText="Collapse">
                  <button
                    tabIndex={-1}
                    onClick={() => setLimitedView(true)}
                    className="p-1"
                  >
                    <ArrowsPointingInIcon className="w-6 h-6 [&>path]:stroke-[2]" />
                  </button>
                </CustomTooltip>
              ))}
            {data.rows.length > 1 && linked_id && (
              <CustomTooltip hoverText="Export">
                <button
                  tabIndex={-1}
                  onClick={() => exportData(linked_id)}
                  className="p-1"
                >
                  <ArrowDownTrayIcon className="w-6 h-6 [&>path]:stroke-[2]" />
                </button>
              </CustomTooltip>
            )}
          </div>
        </div>
      )}
    </Minimizer>
  );
};
