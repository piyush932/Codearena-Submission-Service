const { Worker } = require("bullmq");
const axios = require("axios");

const redisConnection = require("../config/redisConfig");

const Submission = require("../models/submissionModel");

const SOCKET_SERVICE_URL =
  process.env.SOCKET_SERVICE_URL || "http://localhost:3001";

function startEvaluationWorker() {
  const worker = new Worker(
    "EvaluationQueue",
    async (job) => {
      if (job.name !== "EvaluationJob") {
        console.warn("Ignoring unknown evaluation job:", job.name);

        return;
      }

      const {
        submissionId,
        userId,
        status,
        actualOutput,
        expectedOutput,
        error,
        passedTestCases,
        totalTestCases,
        failedTestCaseIndex,
      } = job.data;

      console.log("Received EvaluationJob:", job.id, job.data);

      if (!submissionId) {
        throw new Error("EvaluationJob does not contain submissionId");
      }

      /*
       * Step 1: Persist first.
       * MongoDB is the authoritative final state.
       */
      const updatedSubmission = await Submission.findByIdAndUpdate(
        submissionId,
        {
          status,
          actualOutput: actualOutput || null,
          expectedOutput: expectedOutput || null,
          error: error || null,
          passedTestCases: passedTestCases ?? 0,
          totalTestCases: totalTestCases ?? 0,
          failedTestCaseIndex:
            failedTestCaseIndex === undefined || failedTestCaseIndex === null
              ? null
              : failedTestCaseIndex,
          evaluatedAt: new Date(),
        },
        {
          new: true,
          runValidators: true,
        },
      );

      if (!updatedSubmission) {
        throw new Error(`Submission not found: ${submissionId}`);
      }

      console.log(
        "Submission updated successfully:",
        updatedSubmission._id.toString(),
        updatedSubmission.status,
      );

      /*
       * Step 2: Notify the currently connected client.
       * This must use userId because your Socket Service maps:
       * userId -> socket.id in Redis.
       */
      const socketPayload = {
        submissionId: updatedSubmission._id.toString(),
        problemId: updatedSubmission.problemId,
        status: updatedSubmission.status,
        actualOutput: updatedSubmission.actualOutput,
        expectedOutput: updatedSubmission.expectedOutput,
        error: updatedSubmission.error,
        language: updatedSubmission.language,
        passedTestCases: updatedSubmission.passedTestCases ?? 0,
        totalTestCases: updatedSubmission.totalTestCases ?? 0,
        failedTestCaseIndex: updatedSubmission.failedTestCaseIndex ?? null,
        evaluatedAt: updatedSubmission.evaluatedAt,
      };

      try {
        const socketResponse = await axios.post(
          `${SOCKET_SERVICE_URL}/sendPayload`,
          {
            userId,
            payload: socketPayload,
          },
          {
            timeout: 5000,
          },
        );

        console.log(
          "Socket notification sent:",
          socketResponse.status,
          socketResponse.data,
        );
      } catch (socketError) {
        /*
         * Do not throw here for the MVP.
         * MongoDB already contains the result, so the frontend
         * can recover through GET submission/history API after
         * refresh. Log the failure for debugging.
         */
        console.error(
          "Submission saved, but socket notification failed:",
          socketError.response?.data || socketError.message,
        );
      }

      return {
        submissionId: updatedSubmission._id.toString(),
        userId,
        status: updatedSubmission.status,
      };
    },
    {
      connection: redisConnection,
    },
  );

  worker.on("completed", (job, result) => {
    console.log("EvaluationQueue job completed:", job.id, result);
  });

  worker.on("failed", (job, error) => {
    console.error("EvaluationQueue job failed:", job?.id, error);
  });

  worker.on("error", (error) => {
    console.error("Evaluation worker error:", error);
  });

  console.log("EvaluationQueue worker started");

  return worker;
}

module.exports = startEvaluationWorker;
