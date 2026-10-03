const mongoose = require("mongoose");

const submissionSchema = new mongoose.Schema({
  userId: {
    type: String,
    required: [true, "User id for the submission is missing"],
  },
  problemId: {
    type: String,
    required: [true, "Problem id for the submission is missing"],
  },
  code: {
    type: String,
    required: [true, "Code for the submission is missing"],
  },
  language: {
    type: String,
    required: [true, "Language for the submission is missing"],
  },
  status: {
    type: String,
    enum: ["Pending", "Success", "RE", "TLE", "MLE", "WA"],
    default: "Pending",
  },
  actualOutput: {
    type: String,
    default: null,
  },
  expectedOutput: {
    type: String,
    default: null,
  },
  error: {
    type: String,
    default: null,
  },
  evaluatedAt: {
    type: Date,
    default: null,
  },
  passedTestCases: {
    type: Number,
    default: 0,
  },
  totalTestCases: {
    type: Number,
    default: 0,
  },
  failedTestCaseIndex: {
    type: Number,
    default: null,
  },
});

const Submission = mongoose.model("Submission", submissionSchema);
module.exports = Submission;
