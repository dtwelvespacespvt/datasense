import { enqueueSnackbar } from "notistack";
import { ConversationCreationResult, api } from "@/api";
import {
  MutationOptions,
  useMutation,
  useQuery,
  useQueryClient,
  useInfiniteQuery,
} from "@tanstack/react-query";
import { getBackendStatusQuery } from "@/hooks/settings";
import { useEffect } from "react";
import { useParams } from "@tanstack/react-router";
import { useGetConnections } from "./connections";
import { isAxiosError } from "axios";
import { MESSAGES_QUERY_KEY } from "./messages";
import { IConversationWithMessagesWithResultsOut } from "@/components/Library/types";

export const CONVERSATIONS_QUERY_KEY = ["CONVERSATIONS"];

export function useGetConversations() {
  const { isSuccess } = useQuery(getBackendStatusQuery());
  const result = useQuery({
    queryKey: CONVERSATIONS_QUERY_KEY,
    queryFn: async () => (await api.listConversations()).data,
    enabled: isSuccess,
  });
  const isError = result.isError;

  useEffect(() => {
    if (isError) {
      enqueueSnackbar({
        variant: "error",
        message: "Error loading conversations",
      });
    }
  }, [isError]);

  return result;
}

export function useGetConversationsInfinite(limit: number = 14) {
  const { isSuccess } = useQuery(getBackendStatusQuery());
  const result = useInfiniteQuery({
    queryKey: [...CONVERSATIONS_QUERY_KEY, "infinite"],
    queryFn: async ({ pageParam = 0 }) => {
      const response = await api.listConversations({ skip: pageParam, limit });
      return response.data;
    },
    getNextPageParam: (lastPage, allPages) => {
      const totalFetched = allPages.reduce((total, page) => total + page.length, 0);
      if (lastPage.length < limit) {
        return null;
      }
      return totalFetched;
    },
    enabled: isSuccess,
    initialPageParam: 0,
  });

  const isError = result.isError;

  useEffect(() => {
    if (isError) {
      enqueueSnackbar({
        variant: "error",
        message: "Error loading conversations",
      });
    }
  }, [isError]);

  return result;
}

export function useCreateConversation(
  options: MutationOptions<
    ConversationCreationResult,
    Error,
    { id: string; name: string }
  > = {}
) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, name }: { id: string; name: string }) =>
      api.createConversation(id, name),
    onError() {
      enqueueSnackbar({
        variant: "error",
        message: "Error creating conversation",
      });
    },
    onSettled() {
      queryClient.invalidateQueries({ queryKey: CONVERSATIONS_QUERY_KEY });
    },
    ...options,
  });
}

export function useDeleteConversation(options = {}) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (id: string) => api.deleteConversation(id),

    onMutate: async (idToDelete) => {
      await queryClient.cancelQueries({ queryKey: CONVERSATIONS_QUERY_KEY });

      queryClient.setQueryData(
        [...CONVERSATIONS_QUERY_KEY, "infinite"],
        (oldData: any) => {
          if (!oldData) return oldData;

          return {
            ...oldData,
            pages: oldData.pages.map((page: IConversationWithMessagesWithResultsOut[]) =>
              page.filter((conversation) => conversation.id !== idToDelete)
            ),
          };
        }
      );
      queryClient.setQueryData<IConversationWithMessagesWithResultsOut[]>(CONVERSATIONS_QUERY_KEY, (oldData) => {
        if (!oldData) return [];
        return oldData.filter((conversation) => conversation.id !== idToDelete);
      });
    },
    onError: () => {
      enqueueSnackbar({
        variant: "error",
        message: "Error deleting conversation.",
      });
      queryClient.invalidateQueries({ queryKey: CONVERSATIONS_QUERY_KEY });
    },
    ...options,
  });
}

export function useUpdateConversation(options = {}) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, name }: { id: string; name: string }) =>
      api.updateConversation(id, name),
    onError() {
      enqueueSnackbar({
        variant: "error",
        message: "Error updating conversation",
      });
    },
    onSettled() {
      queryClient.invalidateQueries({ queryKey: CONVERSATIONS_QUERY_KEY });
    },
    ...options,
  });
}

export function useSubmitFeedback(options = {}) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ message_id, is_positive, content }: { message_id: string; is_positive: boolean; content: string }) =>
      api.submitFeedback({ message_id, is_positive, content }),
    onError() {
      enqueueSnackbar({
        variant: "error",
        message: "Error submitting feedback",
      });
    },
    onSettled() {
      queryClient.invalidateQueries({ queryKey: MESSAGES_QUERY_KEY });
    },
    ...options,
  });
}

export function useGenerateConversationTitle(options = {}) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ id }: { id: string }) =>
      (await api.generateConversationTitle(id)).data,
    onError(error) {
      if (isAxiosError(error) && error.response?.status === 400) {
        enqueueSnackbar({
          variant: "error",
          message: error.response.data.detail,
        });
      } else {
        enqueueSnackbar({
          variant: "error",
          message: "There was a problem generating a conversation title",
        });
      }
    },
    onSuccess() {
      queryClient.invalidateQueries({ queryKey: CONVERSATIONS_QUERY_KEY });
    },
    ...options,
  });
}

/**
 * Get the connection object for the current conversation
 *
 * **Warning:** This must be used within the conversation chat context
 *
 * @returns ConnectionResult
 */
export function useGetRelatedConnection() {
  const params = useParams({ from: "/_app/chat/$conversationId" });
  const { data: connectionsData } = useGetConnections();
  const { data: conversationsData } = useGetConversations();
  const currConversation = conversationsData?.find(
    (conv) => conv.id === params.conversationId
  );
  return connectionsData?.connections?.find(
    (conn) => conn.id === currConversation?.connection_id
  );
}
