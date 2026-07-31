export namespace connection {
	
	export class ProbeResult {
	    ok: boolean;
	    latencyMs: number;
	    version?: string;
	
	    static createFrom(source: any = {}) {
	        return new ProbeResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.ok = source["ok"];
	        this.latencyMs = source["latencyMs"];
	        this.version = source["version"];
	    }
	}
	export class Snapshot {
	    connectionId: string;
	    connectionGeneration: string;
	    profileId: string;
	    profileRevision: string;
	    version?: string;
	    protection: protection.Snapshot;
	
	    static createFrom(source: any = {}) {
	        return new Snapshot(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.connectionId = source["connectionId"];
	        this.connectionGeneration = source["connectionGeneration"];
	        this.profileId = source["profileId"];
	        this.profileRevision = source["profileRevision"];
	        this.version = source["version"];
	        this.protection = this.convertValues(source["protection"], protection.Snapshot);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}

}

export namespace exportjob {
	
	export class CommandEnvelope {
	    commandRequestId: string;
	    expectedStateRevision: string;
	
	    static createFrom(source: any = {}) {
	        return new CommandEnvelope(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.commandRequestId = source["commandRequestId"];
	        this.expectedStateRevision = source["expectedStateRevision"];
	    }
	}
	export class Fragment {
	    fragmentId: string;
	    jobId: string;
	    ordinal: string;
	    kind: string;
	    state: string;
	    startNs?: string;
	    endNs?: string;
	    partPath?: string;
	    finalPath?: string;
	    compressedSize?: string;
	    uncompressedSize?: string;
	    checksumSha256?: string;
	    reusable: boolean;
	    createdAt: string;
	    updatedAt: string;
	
	    static createFrom(source: any = {}) {
	        return new Fragment(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.fragmentId = source["fragmentId"];
	        this.jobId = source["jobId"];
	        this.ordinal = source["ordinal"];
	        this.kind = source["kind"];
	        this.state = source["state"];
	        this.startNs = source["startNs"];
	        this.endNs = source["endNs"];
	        this.partPath = source["partPath"];
	        this.finalPath = source["finalPath"];
	        this.compressedSize = source["compressedSize"];
	        this.uncompressedSize = source["uncompressedSize"];
	        this.checksumSha256 = source["checksumSha256"];
	        this.reusable = source["reusable"];
	        this.createdAt = source["createdAt"];
	        this.updatedAt = source["updatedAt"];
	    }
	}
	export class PlanDetail {
	    schemaVersion: number;
	    database: string;
	    retentionPolicy: string;
	    measurement: string;
	    startNs: string;
	    endNs: string;
	    sliceWidthNs: string;
	    outputDirectory: string;
	    typePreserving: boolean;
	    lossy: boolean;
	
	    static createFrom(source: any = {}) {
	        return new PlanDetail(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.schemaVersion = source["schemaVersion"];
	        this.database = source["database"];
	        this.retentionPolicy = source["retentionPolicy"];
	        this.measurement = source["measurement"];
	        this.startNs = source["startNs"];
	        this.endNs = source["endNs"];
	        this.sliceWidthNs = source["sliceWidthNs"];
	        this.outputDirectory = source["outputDirectory"];
	        this.typePreserving = source["typePreserving"];
	        this.lossy = source["lossy"];
	    }
	}
	export class Job {
	    task: tasks.Meta;
	    profileId: string;
	    profileRevision: string;
	    connectionId: string;
	    connectionGeneration: string;
	    specDigest: string;
	    targetDigest: string;
	    targetVolumeId: string;
	    targetReservationId?: string;
	    cancelRequested: boolean;
	    retained: boolean;
	    cleanupFinalState?: string;
	    plan: PlanDetail;
	    fragments?: Fragment[];
	
	    static createFrom(source: any = {}) {
	        return new Job(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.task = this.convertValues(source["task"], tasks.Meta);
	        this.profileId = source["profileId"];
	        this.profileRevision = source["profileRevision"];
	        this.connectionId = source["connectionId"];
	        this.connectionGeneration = source["connectionGeneration"];
	        this.specDigest = source["specDigest"];
	        this.targetDigest = source["targetDigest"];
	        this.targetVolumeId = source["targetVolumeId"];
	        this.targetReservationId = source["targetReservationId"];
	        this.cancelRequested = source["cancelRequested"];
	        this.retained = source["retained"];
	        this.cleanupFinalState = source["cleanupFinalState"];
	        this.plan = this.convertValues(source["plan"], PlanDetail);
	        this.fragments = this.convertValues(source["fragments"], Fragment);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class ListFilter {
	    profileId: string;
	    state: string;
	    limit: number;
	
	    static createFrom(source: any = {}) {
	        return new ListFilter(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.profileId = source["profileId"];
	        this.state = source["state"];
	        this.limit = source["limit"];
	    }
	}

}

export namespace importworker {
	
	export class Result {
	    job: transfer.ImportJob;
	    outcome: string;
	    attemptId?: string;
	    batchId?: string;
	    startOffset?: string;
	    endOffset?: string;
	    pointCount?: string;
	
	    static createFrom(source: any = {}) {
	        return new Result(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.job = this.convertValues(source["job"], transfer.ImportJob);
	        this.outcome = source["outcome"];
	        this.attemptId = source["attemptId"];
	        this.batchId = source["batchId"];
	        this.startOffset = source["startOffset"];
	        this.endOffset = source["endOffset"];
	        this.pointCount = source["pointCount"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}

}

export namespace main {
	
	export class ExportQueryResultInput {
	    sessionId: string;
	    statementId: number;
	    seriesId: string;
	    allRows: boolean;
	    rowIndexes?: string[];
	    suggestedName?: string;

	    static createFrom(source: any = {}) {
	        return new ExportQueryResultInput(source);
	    }

	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.sessionId = source["sessionId"];
	        this.statementId = source["statementId"];
	        this.seriesId = source["seriesId"];
	        this.allRows = source["allRows"];
	        this.rowIndexes = source["rowIndexes"];
	        this.suggestedName = source["suggestedName"];
	    }
	}
	export class ImportSourceInspection {
	    sourcePath: string;
	    displayName: string;
	    sha256: string;
	    sizeBytes: string;
	
	    static createFrom(source: any = {}) {
	        return new ImportSourceInspection(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.sourcePath = source["sourcePath"];
	        this.displayName = source["displayName"];
	        this.sha256 = source["sha256"];
	        this.sizeBytes = source["sizeBytes"];
	    }
	}
	export class PreflightImportInput {
	    clientRequestId: string;
	    profileId: string;
	    sourcePath: string;
	    source: transfer.ImportSourceIdentity;
	    format: string;
	    target: transfer.ImportTarget;
	    csvMapping?: transfer.CSVMapping;
	    numericTextMappings?: transfer.NumericTextFieldMapping[];
	
	    static createFrom(source: any = {}) {
	        return new PreflightImportInput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.clientRequestId = source["clientRequestId"];
	        this.profileId = source["profileId"];
	        this.sourcePath = source["sourcePath"];
	        this.source = this.convertValues(source["source"], transfer.ImportSourceIdentity);
	        this.format = source["format"];
	        this.target = this.convertValues(source["target"], transfer.ImportTarget);
	        this.csvMapping = this.convertValues(source["csvMapping"], transfer.CSVMapping);
	        this.numericTextMappings = this.convertValues(source["numericTextMappings"], transfer.NumericTextFieldMapping);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class ProfileView {
	    id: string;
	    revision: string;
	    name: string;
	    baseUrl: string;
	    defaultDatabase: string;
	    environment: string;
	    authMode: string;
	    username?: string;
	    allowInsecureAuth: boolean;
	    protectionMode: string;
	    createdAt: string;
	    updatedAt: string;
	
	    static createFrom(source: any = {}) {
	        return new ProfileView(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.revision = source["revision"];
	        this.name = source["name"];
	        this.baseUrl = source["baseUrl"];
	        this.defaultDatabase = source["defaultDatabase"];
	        this.environment = source["environment"];
	        this.authMode = source["authMode"];
	        this.username = source["username"];
	        this.allowInsecureAuth = source["allowInsecureAuth"];
	        this.protectionMode = source["protectionMode"];
	        this.createdAt = source["createdAt"];
	        this.updatedAt = source["updatedAt"];
	    }
	}
	export class QueryResultExportView {
	    path: string;
	    rowCount: string;

	    static createFrom(source: any = {}) {
	        return new QueryResultExportView(source);
	    }

	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.path = source["path"];
	        this.rowCount = source["rowCount"];
	    }
	}
	export class QueryScalarView {
	    kind: string;
	    decimalText?: string;
	    value?: any;
	
	    static createFrom(source: any = {}) {
	        return new QueryScalarView(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.kind = source["kind"];
	        this.decimalText = source["decimalText"];
	        this.value = source["value"];
	    }
	}
	export class QueryResultPageView {
	    rows: QueryScalarView[][];
	    nextCursor?: string;
	    eof: boolean;
	
	    static createFrom(source: any = {}) {
	        return new QueryResultPageView(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.rows = this.convertValues(source["rows"], QueryScalarView);
	        this.nextCursor = source["nextCursor"];
	        this.eof = source["eof"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	
	export class RuntimeStatus {
	    ready: boolean;
	    startupCode?: string;
	
	    static createFrom(source: any = {}) {
	        return new RuntimeStatus(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.ready = source["ready"];
	        this.startupCode = source["startupCode"];
	    }
	}
	export class SaveProfileInput {
	    id?: string;
	    expectedRevision?: string;
	    name: string;
	    baseUrl: string;
	    defaultDatabase?: string;
	    environment: string;
	    authMode: string;
	    username?: string;
	    allowInsecureAuth: boolean;
	    protectionMode: string;
	    secret?: string;
	
	    static createFrom(source: any = {}) {
	        return new SaveProfileInput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.expectedRevision = source["expectedRevision"];
	        this.name = source["name"];
	        this.baseUrl = source["baseUrl"];
	        this.defaultDatabase = source["defaultDatabase"];
	        this.environment = source["environment"];
	        this.authMode = source["authMode"];
	        this.username = source["username"];
	        this.allowInsecureAuth = source["allowInsecureAuth"];
	        this.protectionMode = source["protectionMode"];
	        this.secret = source["secret"];
	    }
	}
	export class SchemaField {
	    name: string;
	    type: string;
	
	    static createFrom(source: any = {}) {
	        return new SchemaField(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.type = source["type"];
	    }
	}
	export class SchemaMeasurement {
	    name: string;
	    fields: SchemaField[];
	    tags: string[];
	
	    static createFrom(source: any = {}) {
	        return new SchemaMeasurement(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.fields = this.convertValues(source["fields"], SchemaField);
	        this.tags = source["tags"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class SchemaDatabase {
	    name: string;
	    retentionPolicies: string[];
	    measurements: SchemaMeasurement[];
	
	    static createFrom(source: any = {}) {
	        return new SchemaDatabase(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.retentionPolicies = source["retentionPolicies"];
	        this.measurements = this.convertValues(source["measurements"], SchemaMeasurement);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	
	
	export class StartExportInput {
	    clientRequestId: string;
	    profileId: string;
	    targetDirectory: string;
	    database: string;
	    retentionPolicy?: string;
	    measurements: string[];
	    startNs: string;
	    endNs: string;
	    strict: boolean;
	
	    static createFrom(source: any = {}) {
	        return new StartExportInput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.clientRequestId = source["clientRequestId"];
	        this.profileId = source["profileId"];
	        this.targetDirectory = source["targetDirectory"];
	        this.database = source["database"];
	        this.retentionPolicy = source["retentionPolicy"];
	        this.measurements = source["measurements"];
	        this.startNs = source["startNs"];
	        this.endNs = source["endNs"];
	        this.strict = source["strict"];
	    }
	}
	export class StartReadQueryInput {
	    clientRequestId: string;
	    profileId: string;
	    database: string;
	    retentionPolicy?: string;
	    query: string;
	
	    static createFrom(source: any = {}) {
	        return new StartReadQueryInput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.clientRequestId = source["clientRequestId"];
	        this.profileId = source["profileId"];
	        this.database = source["database"];
	        this.retentionPolicy = source["retentionPolicy"];
	        this.query = source["query"];
	    }
	}
	export class TestConnectionInput {
	    baseUrl: string;
	    authMode: string;
	    username?: string;
	    secret?: string;
	    allowInsecureAuth: boolean;
	
	    static createFrom(source: any = {}) {
	        return new TestConnectionInput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.baseUrl = source["baseUrl"];
	        this.authMode = source["authMode"];
	        this.username = source["username"];
	        this.secret = source["secret"];
	        this.allowInsecureAuth = source["allowInsecureAuth"];
	    }
	}

}

export namespace operation {
	
	export class CommandEnvelope {
	    commandRequestId: string;
	    expectedStateRevision: string;
	
	    static createFrom(source: any = {}) {
	        return new CommandEnvelope(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.commandRequestId = source["commandRequestId"];
	        this.expectedStateRevision = source["expectedStateRevision"];
	    }
	}
	export class ExecuteRequest {
	    clientRequestId: string;
	    previewToken: string;
	    confirmation?: string;
	
	    static createFrom(source: any = {}) {
	        return new ExecuteRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.clientRequestId = source["clientRequestId"];
	        this.previewToken = source["previewToken"];
	        this.confirmation = source["confirmation"];
	    }
	}
	export class Operation {
	    task: tasks.Meta;
	    profileId: string;
	    profileRevision: string;
	    connectionId: string;
	    connectionGeneration: string;
	    protectionRevision: string;
	    actionDigest: string;
	    operationKind: string;
	    dispatchAttempted: boolean;
	    cancelRequested: boolean;
	    replayed?: boolean;
	
	    static createFrom(source: any = {}) {
	        return new Operation(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.task = this.convertValues(source["task"], tasks.Meta);
	        this.profileId = source["profileId"];
	        this.profileRevision = source["profileRevision"];
	        this.connectionId = source["connectionId"];
	        this.connectionGeneration = source["connectionGeneration"];
	        this.protectionRevision = source["protectionRevision"];
	        this.actionDigest = source["actionDigest"];
	        this.operationKind = source["operationKind"];
	        this.dispatchAttempted = source["dispatchAttempted"];
	        this.cancelRequested = source["cancelRequested"];
	        this.replayed = source["replayed"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class Preview {
	    canonicalQuery: string;
	    operationKind: string;
	    target?: string;
	    confirmationRequired: boolean;
	    executable: boolean;
	    token?: string;
	    expiresAt?: string;
	
	    static createFrom(source: any = {}) {
	        return new Preview(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.canonicalQuery = source["canonicalQuery"];
	        this.operationKind = source["operationKind"];
	        this.target = source["target"];
	        this.confirmationRequired = source["confirmationRequired"];
	        this.executable = source["executable"];
	        this.token = source["token"];
	        this.expiresAt = source["expiresAt"];
	    }
	}
	export class PreviewRequest {
	    database: string;
	    retentionPolicy?: string;
	    query: string;
	
	    static createFrom(source: any = {}) {
	        return new PreviewRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.database = source["database"];
	        this.retentionPolicy = source["retentionPolicy"];
	        this.query = source["query"];
	    }
	}

}

export namespace protection {
	
	export class Snapshot {
	    connectionId: string;
	    connectionGeneration: string;
	    protectionRevision: string;
	    mode: string;
	    leaseId?: string;
	    unlockedUntil?: string;
	
	    static createFrom(source: any = {}) {
	        return new Snapshot(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.connectionId = source["connectionId"];
	        this.connectionGeneration = source["connectionGeneration"];
	        this.protectionRevision = source["protectionRevision"];
	        this.mode = source["mode"];
	        this.leaseId = source["leaseId"];
	        this.unlockedUntil = source["unlockedUntil"];
	    }
	}
	export class CommandResult {
	    snapshot: Snapshot;
	    appliedProtectionRevision?: string;
	    replayed: boolean;
	
	    static createFrom(source: any = {}) {
	        return new CommandResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.snapshot = this.convertValues(source["snapshot"], Snapshot);
	        this.appliedProtectionRevision = source["appliedProtectionRevision"];
	        this.replayed = source["replayed"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class LockRequest {
	    connectionId: string;
	    commandRequestId: string;
	    expectedConnectionGeneration: string;
	
	    static createFrom(source: any = {}) {
	        return new LockRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.connectionId = source["connectionId"];
	        this.commandRequestId = source["commandRequestId"];
	        this.expectedConnectionGeneration = source["expectedConnectionGeneration"];
	    }
	}
	
	export class UnlockRequest {
	    connectionId: string;
	    commandRequestId: string;
	    expectedConnectionGeneration: string;
	    expectedProtectionRevision: string;
	
	    static createFrom(source: any = {}) {
	        return new UnlockRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.connectionId = source["connectionId"];
	        this.commandRequestId = source["commandRequestId"];
	        this.expectedConnectionGeneration = source["expectedConnectionGeneration"];
	        this.expectedProtectionRevision = source["expectedProtectionRevision"];
	    }
	}

}

export namespace query {
	
	export class CommandEnvelope {
	    commandRequestId: string;
	    expectedStateRevision: string;
	
	    static createFrom(source: any = {}) {
	        return new CommandEnvelope(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.commandRequestId = source["commandRequestId"];
	        this.expectedStateRevision = source["expectedStateRevision"];
	    }
	}
	export class PageRequest {
	    sessionId: string;
	    statementId: number;
	    seriesId: string;
	    cursor: string;
	    limit: number;
	
	    static createFrom(source: any = {}) {
	        return new PageRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.sessionId = source["sessionId"];
	        this.statementId = source["statementId"];
	        this.seriesId = source["seriesId"];
	        this.cursor = source["cursor"];
	        this.limit = source["limit"];
	    }
	}
	export class QuerySession {
	    id: string;
	    state: string;
	    terminal: boolean;
	    snapshotRevision: string;
	    stateRevision: string;
	    lastEventSeq: string;
	    profileId?: string;
	    profileRevision?: string;
	    connectionId: string;
	    generation: string;
	    createdAt: string;
	    updatedAt: string;
	    terminalAt?: string;
	    publicErrorCode?: string;
	    publicSafeMessage?: string;
	    statementCount: string;
	    seriesCount: string;
	    rowCount: string;
	    hasStatementErrors: boolean;
	    complete: boolean;
	    resultAvailable: boolean;
	    closed: boolean;
	    archived: boolean;
	    replayed?: boolean;
	
	    static createFrom(source: any = {}) {
	        return new QuerySession(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.state = source["state"];
	        this.terminal = source["terminal"];
	        this.snapshotRevision = source["snapshotRevision"];
	        this.stateRevision = source["stateRevision"];
	        this.lastEventSeq = source["lastEventSeq"];
	        this.profileId = source["profileId"];
	        this.profileRevision = source["profileRevision"];
	        this.connectionId = source["connectionId"];
	        this.generation = source["generation"];
	        this.createdAt = source["createdAt"];
	        this.updatedAt = source["updatedAt"];
	        this.terminalAt = source["terminalAt"];
	        this.publicErrorCode = source["publicErrorCode"];
	        this.publicSafeMessage = source["publicSafeMessage"];
	        this.statementCount = source["statementCount"];
	        this.seriesCount = source["seriesCount"];
	        this.rowCount = source["rowCount"];
	        this.hasStatementErrors = source["hasStatementErrors"];
	        this.complete = source["complete"];
	        this.resultAvailable = source["resultAvailable"];
	        this.closed = source["closed"];
	        this.archived = source["archived"];
	        this.replayed = source["replayed"];
	    }
	}
	export class SeriesSummary {
	    id: string;
	    statementId: number;
	    measurement: string;
	    tags?: Record<string, string>;
	    columns: string[];
	    rows: string;
	
	    static createFrom(source: any = {}) {
	        return new SeriesSummary(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.statementId = source["statementId"];
	        this.measurement = source["measurement"];
	        this.tags = source["tags"];
	        this.columns = source["columns"];
	        this.rows = source["rows"];
	    }
	}

}

export namespace tasks {
	
	export class Event {
	    schemaVersion: number;
	    seq: string;
	    kind: string;
	    id: string;
	    resourceRevision: string;
	    state: string;
	    changeType: string;
	    occurredAt: string;
	
	    static createFrom(source: any = {}) {
	        return new Event(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.schemaVersion = source["schemaVersion"];
	        this.seq = source["seq"];
	        this.kind = source["kind"];
	        this.id = source["id"];
	        this.resourceRevision = source["resourceRevision"];
	        this.state = source["state"];
	        this.changeType = source["changeType"];
	        this.occurredAt = source["occurredAt"];
	    }
	}
	export class Meta {
	    id: string;
	    kind: string;
	    state: string;
	    terminal: boolean;
	    snapshotRevision: string;
	    stateRevision: string;
	    lastEventSeq: string;
	    createdAt: string;
	    updatedAt: string;
	    terminalAt?: string;
	    publicErrorCode?: string;
	    publicSafeMessage?: string;
	    archived: boolean;
	    resultAvailable: boolean;
	
	    static createFrom(source: any = {}) {
	        return new Meta(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.kind = source["kind"];
	        this.state = source["state"];
	        this.terminal = source["terminal"];
	        this.snapshotRevision = source["snapshotRevision"];
	        this.stateRevision = source["stateRevision"];
	        this.lastEventSeq = source["lastEventSeq"];
	        this.createdAt = source["createdAt"];
	        this.updatedAt = source["updatedAt"];
	        this.terminalAt = source["terminalAt"];
	        this.publicErrorCode = source["publicErrorCode"];
	        this.publicSafeMessage = source["publicSafeMessage"];
	        this.archived = source["archived"];
	        this.resultAvailable = source["resultAvailable"];
	    }
	}

}

export namespace transfer {
	
	export class AuthorizedResolveRequest {
	    jobId: string;
	    incidentId: string;
	    parentCheckpointDigest: string;
	    commandRequestId: string;
	    expectedStateRevision: string;
	    decision: string;
	    afterResolution?: string;
	    importRunGrant?: string;
	
	    static createFrom(source: any = {}) {
	        return new AuthorizedResolveRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.jobId = source["jobId"];
	        this.incidentId = source["incidentId"];
	        this.parentCheckpointDigest = source["parentCheckpointDigest"];
	        this.commandRequestId = source["commandRequestId"];
	        this.expectedStateRevision = source["expectedStateRevision"];
	        this.decision = source["decision"];
	        this.afterResolution = source["afterResolution"];
	        this.importRunGrant = source["importRunGrant"];
	    }
	}
	export class AuthorizedStartRequest {
	    jobId: string;
	    commandRequestId: string;
	    expectedStateRevision: string;
	    action: string;
	    importRunGrant: string;
	
	    static createFrom(source: any = {}) {
	        return new AuthorizedStartRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.jobId = source["jobId"];
	        this.commandRequestId = source["commandRequestId"];
	        this.expectedStateRevision = source["expectedStateRevision"];
	        this.action = source["action"];
	        this.importRunGrant = source["importRunGrant"];
	    }
	}
	export class CSVFieldMapping {
	    target: string;
	    kind: string;
	
	    static createFrom(source: any = {}) {
	        return new CSVFieldMapping(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.target = source["target"];
	        this.kind = source["kind"];
	    }
	}
	export class CSVMapping {
	    staticMeasurement?: string;
	    measurementColumn?: string;
	    timestampColumn: string;
	    tagColumns?: Record<string, string>;
	    fieldColumns: Record<string, CSVFieldMapping>;
	
	    static createFrom(source: any = {}) {
	        return new CSVMapping(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.staticMeasurement = source["staticMeasurement"];
	        this.measurementColumn = source["measurementColumn"];
	        this.timestampColumn = source["timestampColumn"];
	        this.tagColumns = source["tagColumns"];
	        this.fieldColumns = this.convertValues(source["fieldColumns"], CSVFieldMapping, true);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class Checkpoint {
	    sequence: string;
	    parentDigest?: string;
	    logicalOffset: string;
	    lastSettledBatchId?: string;
	    lastDisposition?: string;
	    adaptiveMaxPoints: string;
	    adaptiveMaxBytes: string;
	    sourceSha256: string;
	    stagingSha256: string;
	    normalizationVersion: string;
	    specDigest: string;
	    targetDigest: string;
	    lossy: boolean;
	
	    static createFrom(source: any = {}) {
	        return new Checkpoint(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.sequence = source["sequence"];
	        this.parentDigest = source["parentDigest"];
	        this.logicalOffset = source["logicalOffset"];
	        this.lastSettledBatchId = source["lastSettledBatchId"];
	        this.lastDisposition = source["lastDisposition"];
	        this.adaptiveMaxPoints = source["adaptiveMaxPoints"];
	        this.adaptiveMaxBytes = source["adaptiveMaxBytes"];
	        this.sourceSha256 = source["sourceSha256"];
	        this.stagingSha256 = source["stagingSha256"];
	        this.normalizationVersion = source["normalizationVersion"];
	        this.specDigest = source["specDigest"];
	        this.targetDigest = source["targetDigest"];
	        this.lossy = source["lossy"];
	    }
	}
	export class ImportCommandEnvelope {
	    commandRequestId: string;
	    expectedStateRevision: string;
	
	    static createFrom(source: any = {}) {
	        return new ImportCommandEnvelope(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.commandRequestId = source["commandRequestId"];
	        this.expectedStateRevision = source["expectedStateRevision"];
	    }
	}
	export class Incident {
	    incidentId: string;
	    kind: string;
	    status: string;
	    parentCheckpointDigest: string;
	    runSegmentId: string;
	    batchId: string;
	    attemptId: string;
	    startOffset: string;
	    endOffset: string;
	    payloadDigest: string;
	    replayParentIncidentId?: string;
	    resolution?: string;
	    resolvedAt?: string;
	    resolutionCommandRequestId?: string;
	
	    static createFrom(source: any = {}) {
	        return new Incident(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.incidentId = source["incidentId"];
	        this.kind = source["kind"];
	        this.status = source["status"];
	        this.parentCheckpointDigest = source["parentCheckpointDigest"];
	        this.runSegmentId = source["runSegmentId"];
	        this.batchId = source["batchId"];
	        this.attemptId = source["attemptId"];
	        this.startOffset = source["startOffset"];
	        this.endOffset = source["endOffset"];
	        this.payloadDigest = source["payloadDigest"];
	        this.replayParentIncidentId = source["replayParentIncidentId"];
	        this.resolution = source["resolution"];
	        this.resolvedAt = source["resolvedAt"];
	        this.resolutionCommandRequestId = source["resolutionCommandRequestId"];
	    }
	}
	export class ImportJob {
	    task: tasks.Meta;
	    profileId: string;
	    checkpointDigest: string;
	    checkpoint: Checkpoint;
	    activeRunSegmentId?: string;
	    pauseRequested: boolean;
	    openIncident?: Incident;
	
	    static createFrom(source: any = {}) {
	        return new ImportJob(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.task = this.convertValues(source["task"], tasks.Meta);
	        this.profileId = source["profileId"];
	        this.checkpointDigest = source["checkpointDigest"];
	        this.checkpoint = this.convertValues(source["checkpoint"], Checkpoint);
	        this.activeRunSegmentId = source["activeRunSegmentId"];
	        this.pauseRequested = source["pauseRequested"];
	        this.openIncident = this.convertValues(source["openIncident"], Incident);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class ImportRunGrant {
	    token: string;
	    expiresAt: string;
	
	    static createFrom(source: any = {}) {
	        return new ImportRunGrant(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.token = source["token"];
	        this.expiresAt = source["expiresAt"];
	    }
	}
	export class ImportRunPreview {
	    jobId: string;
	    state: string;
	    checkpointDigest: string;
	    logicalOffset: string;
	    adaptiveMaxPoints: string;
	    adaptiveMaxBytes: string;
	    targetDigest: string;
	    executable: boolean;
	    importRunGrant?: ImportRunGrant;
	
	    static createFrom(source: any = {}) {
	        return new ImportRunPreview(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.jobId = source["jobId"];
	        this.state = source["state"];
	        this.checkpointDigest = source["checkpointDigest"];
	        this.logicalOffset = source["logicalOffset"];
	        this.adaptiveMaxPoints = source["adaptiveMaxPoints"];
	        this.adaptiveMaxBytes = source["adaptiveMaxBytes"];
	        this.targetDigest = source["targetDigest"];
	        this.executable = source["executable"];
	        this.importRunGrant = this.convertValues(source["importRunGrant"], ImportRunGrant);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class ImportSourceIdentity {
	    sha256: string;
	    sizeBytes: string;
	
	    static createFrom(source: any = {}) {
	        return new ImportSourceIdentity(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.sha256 = source["sha256"];
	        this.sizeBytes = source["sizeBytes"];
	    }
	}
	export class ImportTarget {
	    database: string;
	    retentionPolicy?: string;
	
	    static createFrom(source: any = {}) {
	        return new ImportTarget(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.database = source["database"];
	        this.retentionPolicy = source["retentionPolicy"];
	    }
	}
	
	export class NumericTextFieldMapping {
	    measurement: string;
	    field: string;
	    kind: string;
	
	    static createFrom(source: any = {}) {
	        return new NumericTextFieldMapping(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.measurement = source["measurement"];
	        this.field = source["field"];
	        this.kind = source["kind"];
	    }
	}
	export class PreflightImportResponse {
	    job: ImportJob;
	    replayed: boolean;
	    ready: boolean;
	
	    static createFrom(source: any = {}) {
	        return new PreflightImportResponse(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.job = this.convertValues(source["job"], ImportJob);
	        this.replayed = source["replayed"];
	        this.ready = source["ready"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class PreviewRunRequest {
	    jobId: string;
	    commandRequestId: string;
	    expectedStateRevision: string;
	    action: string;
	    decision?: string;
	    afterResolution?: string;
	
	    static createFrom(source: any = {}) {
	        return new PreviewRunRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.jobId = source["jobId"];
	        this.commandRequestId = source["commandRequestId"];
	        this.expectedStateRevision = source["expectedStateRevision"];
	        this.action = source["action"];
	        this.decision = source["decision"];
	        this.afterResolution = source["afterResolution"];
	    }
	}

}
