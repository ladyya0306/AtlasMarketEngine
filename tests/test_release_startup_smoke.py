import unittest

from scripts import run_release_startup_smoke


class TestReleaseStartupSmokeCli(unittest.TestCase):
    def test_no_default_interventions_flag_disables_defaults(self):
        parser = run_release_startup_smoke._build_arg_parser()

        args = parser.parse_args(["--no-default-interventions"])

        self.assertFalse(args.default_interventions)
        self.assertEqual(run_release_startup_smoke._build_preplanned_interventions(args), [])

    def test_defaults_remain_enabled_without_flag(self):
        parser = run_release_startup_smoke._build_arg_parser()

        args = parser.parse_args([])

        self.assertTrue(args.default_interventions)
        self.assertEqual(
            run_release_startup_smoke._build_preplanned_interventions(args),
            run_release_startup_smoke._default_preplanned_interventions(),
        )


if __name__ == "__main__":
    unittest.main()
