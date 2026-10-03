const { fetchProblemDetails } = require("../apis/problemAdminApi");
const SubmissionCreationError = require("../errors/submissionCreationError");
const SubmissionProducer = require("../producers/submissionQueueProducer");
class SubmissionService {
  constructor(submissionRepository) {
    // inject here
    this.submissionRepository = submissionRepository;
  }

  async pingCheck() {
    return "pong";
  }

  async addSubmission(submissionPayload) {
    const problemId = submissionPayload.problemId;
    const userId = submissionPayload.userId;

    const problemAdminApiResponse = await fetchProblemDetails(problemId);

    const problem = problemAdminApiResponse?.data;

    if (!problem) {
      throw new SubmissionCreationError("Problem details were not found");
    }

    if (!Array.isArray(problem.codeStubs) || problem.codeStubs.length === 0) {
      throw new SubmissionCreationError("Problem does not contain code stubs");
    }

    if (!Array.isArray(problem.testCases) || problem.testCases.length === 0) {
      throw new SubmissionCreationError("Problem does not contain test cases");
    }

    const languageCodeStub = problem.codeStubs.find(
      (codeStub) =>
        codeStub.language.toLowerCase() ===
        submissionPayload.language.toLowerCase(),
    );

    if (!languageCodeStub) {
      throw new SubmissionCreationError(
        `Language ${submissionPayload.language} is not supported`,
      );
    }

    submissionPayload.code =
      `${languageCodeStub.startSnippet || ""}\n\n` +
      `${submissionPayload.code}\n\n` +
      `${languageCodeStub.endSnippet || ""}`;

    const submission =
      await this.submissionRepository.createSubmission(submissionPayload);

    if (!submission) {
      throw new SubmissionCreationError("Failed to create submission");
    }

    const testCases = problem.testCases.map((testCase) => ({
      inputCase: testCase.input,
      outputCase: testCase.output,
    }));

    const response = await SubmissionProducer({
      [submission._id.toString()]: {
        submissionId: submission._id.toString(),
        userId,
        code: submission.code,
        language: submission.language,
        testCases,
      },
    });

    return {
      queueResponse: response,
      submission,
    };
  }
}

module.exports = SubmissionService;
