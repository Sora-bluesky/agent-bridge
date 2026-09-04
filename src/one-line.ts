/*
 * Everything this repository knows about keeping a record on one line.
 *
 * The set below was assembled one character at a time across four
 * rounds of review, and every round found it in a second copy that had
 * fallen behind the first: a name check that knew about U+2028 beside
 * an error path that only knew about CR and LF. Two lists of the same
 * fact drift silently, because the one that is wrong still passes every
 * test written for the one that is right. So the fact is held once here
 * and the operations below are derived from it.
 */

/*
 * The characters a reader ends a record on when it breaks lines the way
 * Python's `str.splitlines()` does. CRLF is not listed: it is CR
 * followed by LF, and both are already here.
 */
export const RECORD_SEPARATOR_CODES = [
  0x000a, 0x000b, 0x000c, 0x000d,
  0x001c, 0x001d, 0x001e, 0x0085,
  0x2028, 0x2029,
] as const;

function unicodeEscape(
  code: number,
): string {
  return `\\u${code
    .toString(16)
    .padStart(4, "0")}`;
}

/*
 * Built from the list rather than written beside it, so a character
 * added above cannot be missing here.
 */
const SEPARATOR = new RegExp(
  `[${RECORD_SEPARATOR_CODES.map(
    unicodeEscape,
  ).join("")}]`,
  "g",
);

/*
 * Escaped rather than dropped or replaced with a space, so the record
 * still says what arrived in it. A reader who sees U+2028 in a name
 * is looking at the reason the name was refused; a reader who sees a
 * space is looking at a name that was never typed.
 *
 * Idempotent: the output holds no separator, so running it again over a
 * line that was already finished changes nothing. That is what lets the
 * emitters below guarantee the line while a formatter upstream keeps
 * escaping its own fields.
 */
export function escapeForOneLine(
  text: string,
): string {
  return text.replace(
    SEPARATOR,
    (character) =>
      unicodeEscape(
        character.charCodeAt(0),
      ),
  );
}

/*
 * `JSON.stringify` escapes the seven separators in the C0 range and
 * writes U+0085, U+2028 and U+2029 out as themselves. Rather than name
 * those three -- a second list, which is the shape of the bug -- the
 * quoting runs the whole set over the result. The seven are already
 * escape sequences by then, so only the three it left can match.
 */
export function quoteForOneLine(
  value: string,
): string {
  return escapeForOneLine(
    JSON.stringify(value),
  );
}

export function errorMessage(
  error: unknown,
): string {
  return error instanceof Error
    ? error.message
    : String(error);
}

/*
 * A line is the thing that has to stay one line, so the guarantee
 * belongs where the line is written rather than at each field that goes
 * into it. Escaping per field was the earlier shape of this, and it
 * covered a subject while the tag beside it went out raw.
 *
 * These two are the only writers to the process streams in `src`, and
 * `doc-check` fails the build if a second one appears. A call site that
 * cannot reach an escaper is the way round five of this bug gets
 * written.
 */
export function writeErrorRecord(
  line: string,
): void {
  process.stderr.write(
    `${escapeForOneLine(line)}\n`,
  );
}

export function writeOutputRecord(
  line: string,
): void {
  process.stdout.write(
    `${escapeForOneLine(line)}\n`,
  );
}
