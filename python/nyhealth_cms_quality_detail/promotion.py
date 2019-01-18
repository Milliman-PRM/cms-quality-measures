"""
### CODE OWNERS: Ben Copeland, Chas Busenburg

### OBJECTIVE:
  Tools to automated some of the manual steps of code promotion

### DEVELOPER NOTES:
  When run as a script, should do the code promotion process
"""
import logging
import os
from pathlib import Path

from indypy.nonstandard.ghapi_tools import repo
from indypy.nonstandard.ghapi_tools import conf
from indypy.nonstandard import promotion_tools

LOGGER = logging.getLogger(__name__)

PATH_RELEASE_NOTES = Path(os.environ["NYHEALTH_CMS_QUALITY_MEASURES_DETAIL"]) / "docs" / "release-notes.md"
PATH_PROMOTION = Path(r"S:\PRM\Pipeline_Components\NYHealth_CMS_Quality_Measures_Detail")

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def main() -> int:
    """Promotion process"""
    LOGGER.info("Beginning code promotion for product component")
    github_repository = repo.GithubRepository.from_parts("NYHealth", "cms-quality-measures")
    version = promotion_tools.LocalVersion(
        input("Please enter the version number for this release (e.g. v1.2.3): "),
        partial=True,
        )
    doc_info = promotion_tools.get_documentation_inputs(github_repository)
    release = promotion_tools.Release(github_repository, version, PATH_PROMOTION, doc_info)
    requested_branch = input("Please input the branch name to promote from (default: master): ")
    if not requested_branch:
        requested_branch = "master"
    assert requested_branch == "master" or version.prerelease,\
        "Cannot promote offical version from anything other than master"
    repository_clone = release.export_repo(branch=requested_branch)
    release.make_release_json()
    if not version.prerelease:
        tag = release.make_tag(repository_clone)
        release.post_github_release(
            conf.get_github_oauth(prompt_if_no_file=True),
            tag,
            body=promotion_tools.get_release_notes(PATH_RELEASE_NOTES, version),
            )
    return 0


if __name__ == '__main__':
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext

    prm.utils.logging_ext.setup_logging_stdout_handler()

    sys.exit(main())
