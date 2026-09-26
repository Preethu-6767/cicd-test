import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { DynamoDBClient, GetItemCommand } from "@aws-sdk/client-dynamodb";

// Read Envs
const awsRegion = process.env.AWS_REGION;
const sqsUrl = process.env.SQS_QUEUE_URL;
const apiKeysTableName = process.env.DYNAMODB_TABLE_NAME;
// If any of the required envs are missing, throw an error.
if (!awsRegion) {
    throw new Error("AWS_REGION is required");
}
if (!sqsUrl) {
    throw new Error("SQS_QUEUE_URL is required");
}
if (!apiKeysTableName) {
    throw new Error("DYNAMODB_TABLE_NAME is required");
}

const sqsClient = new SQSClient({ region: awsRegion });
// Prepare DynamoDB.
const dynamodbClient = new DynamoDBClient({ region: awsRegion });
const apiKeyCache = new Map();
const API_KEY_CACHE_TTL_MS = 1 * 60 * 1000;
const API_KEY_CACHE_MAX_ENTRIES = 1000;

const logPrefix = "[Send Email API]";
export const handler = async (event) => {
    try {

        // console.log(logPrefix, "Incoming:", event.headers, event.body);

        // If content type is not json, reject request.
        if (!isJsonContentType(event.headers)) {
            return prepareResponse(415, {
                status: "ERROR",
                statusCode: 1003,
                message: "Unsupported Media Type. Content-Type must be application/json.",
            });
        }

        // Read Api.
        let apiKey = null;
        if (event.headers["x-kenmail-key"] || event.headers["X-Kenmail-Key"]) {
            apiKey = event.headers["x-kenmail-key"] || event.headers["X-Kenmail-Key"];
        } else if (event.headers["x-onextel-key"] || event.headers["X-Onextel-Key"]) {
            apiKey = event.headers["x-onextel-key"] || event.headers["X-Onextel-Key"];
        } else if (event.headers["authorization"] || event.headers["Authorization"]) {
            apiKey = event.headers["authorization"] || event.headers["Authorization"];
            apiKey = apiKey.toString().replace("Bearer ", "").trim()
        }

        // If API key missing - return error.
        if (!apiKey) {
            console.log(logPrefix, `Authentication failure - API Key is missing`);
            return prepareResponse(403, {
                status: "ERROR",
                statusCode: 9003,
                message: "Authentication failure - API Key is missing",
            });
        }

        // Parse Incoming Payload.
        let parsedPayload;
        try {
            let payload = JSON.parse(event.body);
            // console.log(logPrefix, "Parsing incoming payload:", payload);
            parsedPayload = convertPayload(payload, apiKey);
        } catch (err) {
            console.log(logPrefix, "Failed to parse payload:", err.message);
            return prepareResponse(400, {
                status: "ERROR",
                statusCode: 1003,
                message: "Bad Request.",
            });
        }

        // Meta for logging.
        let requestMeta = parsedPayload.from + "->" + parsedPayload.to + ", jobId: " + parsedPayload.metadata?.jobId || "N/A" + ' APIKEY:' + apiKey;

        // Validate Sender Info.
        try {
            let validationResult = await validateApiKeyAndSenderInfo(apiKey, parsedPayload);
            if (!validationResult.success) {
                console.log(logPrefix, "Validation Failed:", validationResult.message, "Meta: ", requestMeta);
                return prepareResponse(validationResult.httpStatus, {
                    status: "ERROR",
                    statusCode: validationResult.webEngageStatusCode,
                    message: validationResult.message,
                });
            }
        } catch (err) {
            console.log(logPrefix, "Error validating api key and sender info:", err.message, "Meta: ", requestMeta);
            throw new Error("Error validating api key and sender info.", err);
        }

        // Add to queue.
        try {
            // Push to kafka.
            let startTime = new Date();
            let sqsMessageId = await addToQueue(parsedPayload);
            let endTime = new Date();
            let timeTakenInSeconds = ((endTime - startTime) / 1000).toFixed(2);

            // console.log(logPrefix, "Pushed Request:", requestMeta, "Time taken:", timeTakenInSeconds, "secs", 'SQS MsgId:', sqsMessageId);
        } catch (err) {
            console.log(logPrefix, "Failed to push payload to kafka:", err, "Meta: ", requestMeta);
            throw new Error("Error pushing payload to kafka.", "Meta: ", requestMeta);
        }

        // Finished - Return Success Response.
        return prepareResponse(200, {
            status: "SUCCESS",
            statusCode: 1000,
            message: "Email queued successfully",
        });
    } catch (err) {
        console.log(logPrefix, "Internal server error:", err.message);
        return prepareResponse(500, {
            status: "ERROR",
            statusCode: 9988,
            message: "Unknown error occurred",
        });
    }
};

function prepareResponse(httpCode, payload) {
    return {
        statusCode: httpCode,
        headers: {
            "Content-Type": "application/json",
            "Strict-Transport-Security": "max-age=31536000",
        },
        body: JSON.stringify(payload),
    };
}

function isJsonContentType(headers) {
    const contentType = Object.entries(headers ?? {})
        .find(([name]) => name.toLowerCase() === "content-type")?.[1];

    return String(contentType ?? "")
        .split(";")[0]
        .trim()
        .toLowerCase() === "application/json";
}

const convertPayload = (message, apiKey) => {

    let recipients = message.email.recipients;
    // console.log("Recipients are: ", recipients);

    // Extract recipients properly
    const toRecipients = message.email.recipients?.to?.map((r) => r.email).filter(Boolean) || [];
    const ccRecipients = message.email.recipients?.cc || [];
    const bccRecipients = message.email.recipients?.bcc || [];

    // Initialize Email Send Payload
    let emailSendPayload = {};
    emailSendPayload.to = toRecipients.join(",");

    if (ccRecipients.length > 0) {
        emailSendPayload.cc = ccRecipients.join(",");
    }
    if (bccRecipients.length > 0) {
        emailSendPayload.bcc = bccRecipients.join(",");
    }

    emailSendPayload.from = message.email.from;
    emailSendPayload.from_description = message.email.fromName;
    emailSendPayload.reply_to = message.email.replyTo?.[0] ?? message.email.from;
    emailSendPayload.subject = message.email.subject;
    emailSendPayload.html = message.email.html;
    emailSendPayload.amp_html = message.email.amp_html || null;
    emailSendPayload.attachments = message.email.attachments || [];
    emailSendPayload.metadata = message.metadata || null;

    emailSendPayload.apiKey = apiKey;
    emailSendPayload.version = message.version || null;

    // Return Email Send Payload
    return emailSendPayload;
};

const isValidEmail = (email) => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
};

// Each Lambda execution environment keeps its own five-minute cache.
const getApiKeyDomains = async (apiKey) => {
    const cached = apiKeyCache.get(apiKey);
    if (cached && cached.expiresAt > Date.now()) {
        return cached.domains;
    }
    apiKeyCache.delete(apiKey);

    const { Item } = await dynamodbClient.send(new GetItemCommand({
        TableName: apiKeysTableName,
        Key: { apiKey: { S: apiKey } },
        ConsistentRead: true,
    }));
    if (!Item) {
        return null;
    }

    // DynamoDB item: { apiKey: "your-api-key", domains: ["example.com"] }
    const domainValues = Item.domains?.L;
    if (!Array.isArray(domainValues) || domainValues.some((value) => typeof value?.S !== "string")) {
        throw new Error("DynamoDB domains must be a list of strings");
    }
    const domains = domainValues.map((value) => value.S);

    // Bound memory use as the number of configured API keys grows.
    if (apiKeyCache.size >= API_KEY_CACHE_MAX_ENTRIES) {
        apiKeyCache.delete(apiKeyCache.keys().next().value);
    }
    apiKeyCache.set(apiKey, {
        domains,
        expiresAt: Date.now() + API_KEY_CACHE_TTL_MS,
    });
    return domains;
};

const validateApiKeyAndSenderInfo = async (apiKey, parsedPayload) => {
    const allowedDomains = await getApiKeyDomains(apiKey);

    // If Api Key Is Invalid.
    if (allowedDomains === null) {
        return {
            success: false,
            httpStatus: 403,
            webEngageStatusCode: 9003,
            message: "Authentication failure - Invalid API Key",
        };
    }

    // From address - Missing or invalid.
    if (!parsedPayload.from) {
        return {
            success: false,
            httpStatus: 400,
            webEngageStatusCode: 9005,
            message: "From field missing",
        };
    } else {
        let fromAddress = parsedPayload.from;
        let senderDomain = fromAddress.split("@")[1];
        const replyTo = parsedPayload.reply_to;
        if (!allowedDomains.includes(senderDomain)) {
            return {
                success: false,
                httpStatus: 400,
                webEngageStatusCode: 9011,
                message: "Sender address not verified",
            };
        } else if (!isValidEmail(fromAddress)) {
            return {
                success: false,
                httpStatus: 400,
                webEngageStatusCode: 9017,
                message: "Invalid sender address",
            };
        } else if (replyTo !== undefined && replyTo !== null && (typeof replyTo !== "string" || !isValidEmail(replyTo))) {
            return {
                success: false,
                httpStatus: 400,
                webEngageStatusCode: 1003,
                message: "Invalid reply-to email address",
            };
        }
    }

    // Recipient address - Missing or invalid.
    if (!parsedPayload.to) {
        return {
            success: false,
            httpStatus: 400,
            webEngageStatusCode: 9004,
            message: "Recipient address not specified",
        };
    } else {
        // If invalid recipient email address format return error.
        let recipientEmails = parsedPayload.to.split(",").map((email) => email.trim());
        for (let email of recipientEmails) {
            if (!isValidEmail(email)) {
                return {
                    success: false,
                    httpStatus: 400,
                    webEngageStatusCode: 9018,
                    message: `Invalid recipient email address format: ${email}`,
                };
            }
        }
    }

    // Subject field empty.
    if (!parsedPayload.subject) {
        return {
            success: false,
            httpStatus: 400,
            webEngageStatusCode: 9016,
            message: "Email subject is required",
        };
    }

    // Version - missing or unsupported.
    let version = parsedPayload.version;
    if (!version || ["1.0", "2.0"].includes(version) === false) {
        return {
            success: false,
            httpStatus: 400,
            webEngageStatusCode: 9022,
            message: "Unsupported API version",
        };
    }

    return {
        success: true,
        httpStatus: 200,
        webEngageStatusCode: 1000,
        message: "API Key and sender email are valid",
    };
};

const addToQueue = async (payload) => {
    const params = {
        QueueUrl: sqsUrl,
        MessageBody: JSON.stringify(payload),
        // Optional: For FIFO queues, uncomment and set these:
        // MessageGroupId: "default",
        // MessageDeduplicationId: Date.now().toString(),
    };
    const command = new SendMessageCommand(params);
    const result = await sqsClient.send(command);
    return result.MessageId;
};
