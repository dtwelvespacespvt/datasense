import logo from "@/assets/images/logo_md.png";
import { IMessageWithResultsOut, IMessageOptions } from "@components/Library/types";
import { UserCircleIcon } from "@heroicons/react/24/solid";
import { useGetAvatar, useSubmitFeedback } from "@/hooks";
import { HandThumbDownIcon, HandThumbUpIcon, ShieldCheckIcon } from "@heroicons/react/24/outline";
import { InfoTooltip } from "@components/Library/Tooltip";
import { MessageResultRenderer } from "./MessageResultRenderer";
import { Spinner } from "../Spinner/Spinner";
import React from "react";

function classNames(...classes: string[]) {
  return classes.filter(Boolean).join(" ");
}

const MessageIcon = ({ message }: { message: IMessageWithResultsOut }) => {
  const { data: avatarUrl } = useGetAvatar();

  return message.message.role === "ai" ? (
    <div className="flex flex-col shrink-0 items-center mt-2 ">
      <img src={logo} className="h-8 w-8 rounded-md" />
      {message.message.options?.secure_data && (
        <a href="https://dataline.app/faq" target="_blank">
          <InfoTooltip hoverText="No data was sent to or processed by the AI in this message. Click to learn more about how we do this.">
            <div className="text-green-400/90 mt-3 bg-green-400/20 rounded-full hover:bg-green-400/40 transition-colors duration-150 cursor-pointer p-1">
              <ShieldCheckIcon className="w-6 h-6" />
            </div>
          </InfoTooltip>
        </a>
      )}
    </div>
  ) : avatarUrl ? (
    <img
      className="h-8 w-8 rounded-md mt-2 object-cover"
      src={avatarUrl}
      alt=""
    />
  ) : (
    <UserCircleIcon className="text-gray-300 h-8 w-8 mt-1 rounded-full" />
  );
};

export const Message = ({
  message,
  className = "",
  streaming = false,
  messageOptions,
}: {
  message: IMessageWithResultsOut;
  className?: string;
  streaming?: boolean;
  messageOptions?: IMessageOptions;
}) => {
  const [feedback, setFeedback] = React.useState<{ isPositive: boolean | null; text: string }>({
    isPositive: null,
    text: "",
  });
  const [showFeedbackInput, setShowFeedbackInput] = React.useState(false);

  const parseMessageContent = (text: string) => {
    const parts: React.ReactNode[] = [];
    let lastIndex = 0;
    const regex = /<([^>]+)>|\[([^\]]+)\]/g;
    let match;

    while ((match = regex.exec(text)) !== null) {
      const before = text.slice(lastIndex, match.index);
      if (before) parts.push(before);
      if (match[1]) {
        parts.push(
          <span
            className="rounded-md bg-gray-700/40 px-2 py-1 font-medium text-gray-400 ring-1 ring-inset ring-white/10"
            key={match.index}
          >
            {match[1]}
          </span>
        );
      } else if (match[2]) {
        parts.push(
          <strong
            className="rounded-md bg-gray-700/40 px-2 py-1 font-medium text-gray-400 ring-1 ring-inset ring-white/10"
            key={match.index}
          >
            {match[2]}
          </strong>
        );
      }
      lastIndex = regex.lastIndex;
    }

    const after = text.slice(lastIndex);
    if (after) parts.push(after);
    return parts;
  };

  const { mutate: submitFeedback } = useSubmitFeedback({
    onSuccess() {
      console.log("Feedback submitted successfully");
    },
  });

  const handleThumbClick = (isPositive: boolean) => {
    // store the chosen feedback rating
    if (showFeedbackInput) {
      setShowFeedbackInput(false);
      return;
    }
    setFeedback({ ...feedback, isPositive });
    setShowFeedbackInput(true);
  };

  return (
    <div
      className={classNames(
        message.message.role === "ai" ? "dark:bg-gray-700/30" : "dark:bg-gray-900",
        "w-full text-gray-800 dark:text-gray-100 bg-gray-50",
        className
      )}
    >
      <div className="text-base md:max-w-2xl lg:max-w-2xl xl:max-w-2xl py-4 md:py-6 lg:px-0 m-auto">
        <div className="px-3 md:px-1 w-full flex flex-col gap-2 md:gap-6 scrollbar-hide">
          {/** RESULTS: QUERY, DATA, PLOTS */}
          <div className="px-2 md:px-0 flex gap-2 sm:gap-4 md:gap-6">
            <div className="flex flex-col shrink-0">
              <MessageIcon message={message} />
            </div>
            <div className="flex flex-col gap-2 md:gap-6 min-w-0 flex-1">
              <MessageResultRenderer initialResults={message.results || []} messageId={message.message.id || ""} messageOptions={messageOptions} />
              {message.message.content && (
                <div className="min-h-[20px] flex whitespace-pre-wrap break-words">
                  <div className="markdown prose w-full break-words dark:prose-invert dark">
                    <div className="flex gap-2">
                      {streaming && (
                        <div className="flex items-center">
                          <Spinner />
                        </div>
                      )}
                      <p className="leading-loose w-full break-words">{parseMessageContent(message.message.content)}</p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
          <div className="flex flex-col justify-end items-end">
            {message.message.role === "ai" ? (
              <div className="flex flex-row">
                <HandThumbUpIcon
                  onClick={() => {
                    if (message.message.is_positive == null || message.message.is_positive === false)
                      handleThumbClick(true);
                  }}
                  className={`cursor-pointer mx-5 w-5 h-5 ${message.message.is_positive === true ? "text-green-400" : "text-gray-400 hover:text-green-400"
                    }`}
                />
                <HandThumbDownIcon
                  onClick={() => {
                    if (message.message.is_positive == null || message.message.is_positive === true)
                      handleThumbClick(false);
                  }}
                  className={`cursor-pointer w-5 h-5 ${message.message.is_positive === false ? "text-red-400" : "text-gray-400 hover:text-red-400"
                    }`}
                />
              </div>
            ) : null}
          </div>
          {showFeedbackInput && (
            <div className="mt-2 flex flex-col">
              <textarea
                className="p-2 border border-gray-300 rounded-md bg-inherit active:border-indigo-100"
                value={feedback.text}
                onChange={(e) => setFeedback({ ...feedback, text: e.target.value })}
                placeholder="Optional feedback..."
              />
              <button
                onClick={() => {
                  submitFeedback({
                    message_id: message.message.id || "",
                    is_positive: feedback.isPositive as boolean,
                    content: feedback.text,
                  });
                  setShowFeedbackInput(false);
                }}
                className="mt-1 border border-gray-300 text-white px-3 py-1 rounded"
              >
                Submit
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
