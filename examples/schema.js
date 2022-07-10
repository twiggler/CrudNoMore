"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.publicT2IdBypublicUT2 = exports.publicT1IdBypublicUT1 = exports.publicT1IdBypublicT2T1 = exports.publicDocumentIdBypublicT1Document = exports.publicUData = exports.publicUT2 = exports.publicUT1 = exports.publicUId = exports.publicT2Id = exports.publicT2T1 = exports.publicT1Id = exports.publicT1Document = exports.publicDocumentId = void 0;
exports.publicDocumentId = {
    name: "id",
    qualifiedTableName: "public.document",
    dbType: "bigint",
    isPrimary: true,
};
exports.publicT1Document = {
    name: "document",
    qualifiedTableName: "public.t1",
    dbType: "bigint",
    isPrimary: false,
};
exports.publicT1Id = {
    name: "id",
    qualifiedTableName: "public.t1",
    dbType: "bigint",
    isPrimary: true,
};
exports.publicT2T1 = {
    name: "t1",
    qualifiedTableName: "public.t2",
    dbType: "bigint",
    isPrimary: false,
};
exports.publicT2Id = {
    name: "id",
    qualifiedTableName: "public.t2",
    dbType: "bigint",
    isPrimary: true,
};
exports.publicUId = {
    name: "id",
    qualifiedTableName: "public.u",
    dbType: "bigint",
    isPrimary: true,
};
exports.publicUT1 = {
    name: "t1",
    qualifiedTableName: "public.u",
    dbType: "bigint",
    isPrimary: false,
};
exports.publicUT2 = {
    name: "t2",
    qualifiedTableName: "public.u",
    dbType: "bigint",
    isPrimary: false,
};
exports.publicUData = {
    name: "data",
    qualifiedTableName: "public.u",
    dbType: "text",
    isPrimary: false,
};
exports.publicDocumentIdBypublicT1Document = {
    from: exports.publicT1Document,
    to: exports.publicDocumentId,
};
exports.publicT1IdBypublicT2T1 = { from: exports.publicT2T1, to: exports.publicT1Id };
exports.publicT1IdBypublicUT1 = { from: exports.publicUT1, to: exports.publicT1Id };
exports.publicT2IdBypublicUT2 = { from: exports.publicUT2, to: exports.publicT2Id };
