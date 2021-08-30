export type TupleSingle<Tuple extends readonly Base[], Base = any> = Tuple extends [
	infer HeadElement
]
	? HeadElement extends Base
		? HeadElement
		: never
	: never;

export type TupleHead<Tuple extends readonly Base[], Base = any> = Tuple extends [
	infer HeadElement,
	...(readonly any[])
]
	? HeadElement extends Base
		? HeadElement
		: never
	: never;

export type TupleTail<Tuple extends readonly Base[], Base = any> = Tuple extends [
	Base,
	...infer TailElements
]
	? TailElements extends readonly Base[]
		? TailElements
		: never
	: never;

export type TupleAppend<Tuple extends readonly Base[], NewElement extends Base, Base = any> = [
	...Tuple,
	NewElement
];
