/*DO NOT EDIT - THIS IS A GENERATED FILE*/
import { Column, Reference } from "../schema";
export type publicDocumentId = Column<"id", "public.document", number, true>;
export const publicDocumentId: publicDocumentId = {
	name: "id",
	qualifiedTableName: "public.document",
	isPrimary: true,
};
export type publicT1Document = Column<"document", "public.t1", number, false>;
export const publicT1Document: publicT1Document = {
	name: "document",
	qualifiedTableName: "public.t1",
	isPrimary: false,
};
export type publicT1Id = Column<"id", "public.t1", number, true>;
export const publicT1Id: publicT1Id = {
	name: "id",
	qualifiedTableName: "public.t1",
	isPrimary: true,
};
export type publicT2T1 = Column<"t1", "public.t2", number, false>;
export const publicT2T1: publicT2T1 = {
	name: "t1",
	qualifiedTableName: "public.t2",
	isPrimary: false,
};
export type publicT2Id = Column<"id", "public.t2", number, true>;
export const publicT2Id: publicT2Id = {
	name: "id",
	qualifiedTableName: "public.t2",
	isPrimary: true,
};
export type publicUId = Column<"id", "public.u", number, true>;
export const publicUId: publicUId = { name: "id", qualifiedTableName: "public.u", isPrimary: true };
export type publicUT1 = Column<"t1", "public.u", number, false>;
export const publicUT1: publicUT1 = {
	name: "t1",
	qualifiedTableName: "public.u",
	isPrimary: false,
};
export type publicUT2 = Column<"t2", "public.u", number, false>;
export const publicUT2: publicUT2 = {
	name: "t2",
	qualifiedTableName: "public.u",
	isPrimary: false,
};
export type publicUData = Column<"data", "public.u", string, false>;
export const publicUData: publicUData = {
	name: "data",
	qualifiedTableName: "public.u",
	isPrimary: false,
};
export const publicDocumentIdBypublicT1Document: Reference = {
	from: publicT1Document,
	to: publicDocumentId,
};

export const publicT1IdBypublicT2T1: Reference = { from: publicT2T1, to: publicT1Id };
export const publicT1IdBypublicUT1: Reference = { from: publicUT1, to: publicT1Id };
export const publicT2IdBypublicUT2: Reference = { from: publicUT2, to: publicT2Id };