from pipefunc import PipeFunc, Pipeline


def md5_hash(text_to_be_hashed):
    import hashlib

    return hashlib.md5(str(text_to_be_hashed).encode("utf-8")).hexdigest()


def markdown_make_bold(text_to_be_made_bold):
    return f"**{text_to_be_made_bold}**"


def example_pipeline():
    function_to_output_name_map = {
        md5_hash: "hashed_text",
        markdown_make_bold: "bold_string",
    }

    # pipeline_A = Pipeline([
    #     PipeFunc(md5_hash, "hashed_text", mapspec="text_to_be_hashed[n] -> hashed_text[n]"),
    #     PipeFunc(markdown_make_bold, "bold_string", renames={"text_to_be_made_bold": "hashed_text"}, mapspec="hashed_text[n] -> bold_string[n]"),
    # ])

    pipeline_B = Pipeline(
        [
            PipeFunc(
                md5_hash,
                output_name="hashed_text",
                mapspec="text_to_be_hashed[n] -> hashed_text[n]",
                defaults={"text_to_be_hashed": "123"},
                profile=True,
                debug=True,
            ),
            PipeFunc(
                markdown_make_bold,
                output_name="bold_string",
                mapspec="hashed_text[n] -> bold_string[n]",
                profile=True,
                debug=True,
                renames={"text_to_be_made_bold": "hashed_text"},
            ),
        ]
    )

    # # results_A = pipeline_A.map(inputs={"text_to_be_hashed": [1,2,3]})
    # results_B = pipeline_B.map(inputs={"text_to_be_hashed": [1,2,3]})
    # pipeline_B.print_documentation()
    # # print(f"{results_A=}")
    # print(f"{results_B=}")


if __name__ == "__main__":
    example_pipeline()
