export type Result<T, E = Error> = Ok<T, E> | Err<T, E>;

export class Ok<T, E> {
  readonly isOk = true as const;
  readonly isErr = false as const;
  constructor(readonly value: T) {}
}

export class Err<T, E> {
  readonly isOk = false as const;
  readonly isErr = true as const;
  constructor(readonly error: E) {}
}

export const Result = {
  ok: <T, E = Error>(value: T): Result<T, E> => new Ok(value),
  err: <T, E = Error>(error: E): Result<T, E> => new Err(error),
};
