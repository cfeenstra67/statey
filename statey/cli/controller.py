import asyncio
import contextlib
import dataclasses as dc
import importlib
import logging
import os
import sys
import types
from importlib.machinery import SourceFileLoader
from typing import Tuple, Optional

import statey as st
from statey.executor import AsyncIOGraphExecutor
from statey.cli.graph_utils import Inspector, ExecutorLoggingPlugin

import click
import shutil


LOGGER = logging.getLogger(__name__)


class Controller:
    """
    Controller class for the statey CLI
    """

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        terminal_size: Optional[Tuple[int, int]] = None,
        registry: Optional[st.Registry] = None,
        fulltrace: bool = False,
        show_metatasks: bool = False,
        show_task_dag: bool = True,
        show_diff: bool = False,
        task_heartbeat: int = 15.0,
        conf_name: str = "statey_conf.py",
        env_prefix: str = "STATEY_",
    ) -> None:

        if registry is None:
            registry = st.registry

        if logger is None:
            logger = LOGGER

        if terminal_size is None:
            terminal_size = shutil.get_terminal_size((80, 20))

        self.logger = logger
        self.terminal_size = terminal_size
        self.fulltrace = fulltrace
        self.conf_name = conf_name
        self.env_prefix = env_prefix
        self.show_metatasks = show_metatasks
        self.task_heartbeat = task_heartbeat
        self.registry = registry
        self.show_task_dag = show_task_dag
        self.show_diff = show_diff

        self.inspector = Inspector()

        self.state_manager = None
        self.resource_graph = None
        self.session = None
        self.conf_module = None
        self.module_name = None
        self.session_name = None
        self.plan = None
        self.plan_resource_graph = None
        self.exec_info = None

    def setup_state_manager(self) -> None:
        """

        """
        if self.state_manager is not None:
            return
        self.setup_config()
        try:
            self.state_manager = self.registry.get_state_manager()
        except Exception as err:
            self.logger.exception(
                "Error obtaining state manager: %s: %s", type(err).__name__, err
            )
            raise click.Abort from err

        self.logger.debug(
            "Successfully instantiated state manager: %r", self.state_manager
        )

    def setup_resource_graph(self) -> None:
        """

        """
        if self.resource_graph is not None:
            return
        self.setup_state_manager()
        try:
            self.resource_graph = self.state_manager.load(self.registry)
        except Exception as err:
            self.logger.exception(
                "Error loading state: %s: %s", type(err).__name__, err
            )
            raise click.Abort from err

        self.logger.debug(
            "Successfully loaded resource graph with %r", self.state_manager
        )

    def import_module_from_name(
        self, name: str, descriptor: str = "module", allow_fail: bool = False
    ) -> types.ModuleType:
        """
        Import a module by name, w/ proper error handling
        """
        # First attempt to load the module
        # First attempt to load a module with this name. Failing that, try to load
        # the name as a source file
        module = None
        try:
            module = importlib.import_module(name)
        except ImportError:
            pass
        except Exception as err:
            self.logger.exception(
                "Error importing %s '%s': %s: %s",
                descriptor,
                name,
                type(err).__name__,
                err,
            )
            raise click.Abort from err
        else:
            self.logger.debug("Imported %s successfully.", name)

        if module is None:
            loader = SourceFileLoader("__main__", name)
            module = types.ModuleType(loader.name)
            try:
                loader.exec_module(module)
            except (ImportError, FileNotFoundError) as err:
                if not allow_fail:
                    self.logger.exception(
                        "Error importing %s: '%s': %s: %s",
                        descriptor,
                        name,
                        type(err).__name__,
                        err,
                    )
                    raise click.Abort from err
                return None
            except Exception as err:
                self.logger.exception(
                    "Error importing %s: '%s': %s: %s",
                    descriptor,
                    name,
                    type(err).__name__,
                    err,
                )
                raise click.Abort from err
            else:
                self.logger.debug("Imported %s successfully", name)

        return module

    def setup_config(self) -> None:
        """

        """
        if self.conf_module is not None:
            return None

        module = self.import_module_from_name(self.conf_name, allow_fail=True)
        self.module_name = getattr(
            module,
            "MODULE_NAME",
            os.getenv(self.env_prefix + "MODULE_NAME", "statey_module.py"),
        )
        self.session_name = getattr(
            module,
            "SESSION_NAME",
            os.getenv(self.env_prefix + "SESSION_NAME", "session()"),
        )
        self.conf_module = module

        self.logger.debug(
            "Loading configuration module '%s' successfully.", self.conf_name
        )

    def setup_session(self) -> None:
        """

        """
        if self.session is not None:
            return

        self.setup_config()
        module = self.import_module_from_name(self.module_name)

        if self.session_name.endswith("()"):
            value_name = self.session_name[:-2]
            try:
                session_factory = getattr(module, value_name)
            except AttributeError as err:
                self.logger.exception(
                    "Error loading session '%s': no value named %s in module %s",
                    self.session_name,
                    value_name,
                    self.module_name,
                )
                raise click.Abort from err

            try:
                self.session = session_factory()
            except Exception as err:
                self.logger.exception(
                    "Error loading session '%s' from factory %r: %s: %s",
                    self.session_name,
                    session_factory,
                    type(err).__name__,
                    err,
                )
                raise click.Abort from err
            else:
                self.logger.debug(
                    "Loaded %s from %s successfully.",
                    self.session_name,
                    self.module_name,
                )

        else:
            try:
                self.session = getattr(module, self.session_name)
            except AttributeError as err:
                self.logger.exception(
                    "Error loading session '%s': no value named %s in module %s",
                    self.session_name,
                    self.session_name,
                    self.module_name,
                )
                raise click.Abort from err
            else:
                self.logger.debug(
                    "Loaded %s from %s successfully.",
                    self.session_name,
                    self.module_name,
                )

    def refresh_resource_graph(self, progressbar=True, **kwargs) -> None:
        """

        """
        self.setup_resource_graph()
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(
                st.helpers.refresh(
                    self.resource_graph, progressbar=progressbar, **kwargs
                )
            )
        except Exception as err:
            self.logger.exception(
                "Error refreshing resource graph: %s: %s", type(err).__name__, err
            )
            raise click.Abort from err
        else:
            self.logger.debug("Resource graph refreshed successfully.")

    def setup_plan(self) -> None:
        """
        
        """
        if self.plan is not None:
            return

        self.setup_resource_graph()
        self.setup_session()

        loop = asyncio.get_event_loop()
        try:
            self.plan = loop.run_until_complete(
                st.helpers.plan(
                    session=self.session,
                    resource_graph=self.resource_graph,
                    refresh=True,
                    refresh_progressbar=True,
                )
            )
            self.plan_resource_graph = self.plan.task_graph.resource_graph
        except Exception as err:
            self.logger.exception(
                "Error occurred during planning: %s: %s", type(err).__name__, err
            )
            raise click.Abort from err

    @contextlib.contextmanager
    def wrapped_providers_context(self):
        """

        """
        self.setup_plan()

        ctx = st.helpers.providers_context(self.plan.providers)
        try:
            ctx.__enter__()
        except Exception as err:
            self.logger.exception(
                "Error setting up providers: %s: %s", type(err).__name__, err
            )
            raise click.Abort from err

        try:
            yield
        finally:
            try:
                ctx.__exit__(*sys.exc_info())
            except Exception as err:
                self.logger.exception(
                    "Error tearing down providers: %s: %s", type(err).__name__, err
                )
                raise click.Abort from err

    def execute_plan(self) -> None:
        """

        """
        self.setup_plan()

        executor = AsyncIOGraphExecutor()
        executor.pm.register(
            ExecutorLoggingPlugin(self.show_metatasks, self.task_heartbeat)
        )

        with self.wrapped_providers_context():
            try:
                self.exec_info = executor.execute(self.plan.task_graph)
            except Exception as err:
                self.logger.exception(
                    "Unhandled error occurred during execution: %s: %s",
                    type(err).__name__,
                    err,
                )
                raise click.Abort from err

        # Right now plans can only be execute once, so get rid of it after execution
        self.plan = None

    def print_execution_summary(self) -> None:
        """
        Print out execution summary
        """
        if self.exec_info is None:
            raise ValueError("Plan has not been executed.")

        try:
            exec_summary = self.inspector.execution_summary(
                self.exec_info, self.show_metatasks
            )
            exec_summary_string = exec_summary.to_string(full_trace=self.fulltrace)
        except Exception as err:
            self.logger.exception(
                "Error generating execution summary: %s: %s", type(err).__name__, err
            )
            raise click.Abort from err

        self.logger.info(exec_summary_string)

    def print_plan_summary(self) -> None:
        """

        """
        self.setup_plan()

        if self.plan.is_empty():
            self.logger.info(
                click.style("This plan is empty :)", fg="green", bold=True)
            )
            self.logger.info("")
            return

        try:
            plan_summary = self.inspector.plan_summary(self.plan, self.show_metatasks)
        except Exception as err:
            self.logger.exception(
                "Error generating plan summary: %s: %s", type(err).__name__, err
            )
            raise click.Abort from err

        if self.show_diff:
            try:
                plan_summary_string = plan_summary.to_string(self.terminal_size.columns)
            except Exception as err:
                self.logger.exception(
                    "Error generating plan summary string: %s: %s",
                    type(err).__name__,
                    err,
                )
                raise click.Abort from err

            self.logger.info(plan_summary_string)
            self.logger.info("")

        if self.show_task_dag:
            try:
                task_dag_string = plan_summary.task_dag_string()
            except Exception as err:
                self.logger.exception(
                    "Error generating task DAG string: %s: %s", type(err).__name__, err
                )
                raise click.Abort from err
            self.logger.info(
                f"{click.style('Task DAG', fg='green', bold=True)}:\n\n%s\n",
                task_dag_string,
            )

    def dump_state(self) -> None:
        """

        """
        self.setup_state_manager()

        resource_graph = self.resource_graph
        if self.plan_resource_graph is not None:
            resource_graph = self.plan_resource_graph

        try:
            self.state_manager.dump(resource_graph, self.registry)
        except Exception as err:
            self.logger.exception(
                "Error storing resource graph: %s: %s", type(err).__name__, err
            )
            raise click.Abort from err

    def load_provider(self, name: str) -> st.Provider:
        """

        """
        try:
            return self.registry.get_provider(name)
        except Exception as err:
            self.logger.exception(
                "Error getting provider '%s': %s: %s", name, type(err).__name__, err
            )
            raise click.Abort from err

    def load_resource(self, provider: st.Provider, name: str) -> st.Resource:
        """

        """
        try:
            return provider.get_resource(name)
        except Exception as err:
            self.logger.exception(
                "Error loading resource '%s' from provider '%s': %s: %s",
                name,
                provider.id.name,
                type(err).__name__,
                err,
            )
            raise click.Abort from err

    def print_resource_docs(self, resource: st.Resource) -> None:
        """

        """
        name_style = {"fg": "green", "bold": True}

        output_type_serializer = st.registry.get_type_serializer(
            resource.UP.output_type
        )
        output_json = output_type_serializer.serialize(resource.UP.output_type)

        input_type_serializer = st.registry.get_type_serializer(resource.UP.input_type)
        input_json = input_type_serializer.serialize(resource.UP.input_type)

        click.echo(click.style("Name:", **name_style) + f" {resource}")
        click.echo(
            click.style("Inputs:", **name_style)
            + "\n"
            + json.dumps(input_json, indent=2, sort_keys=True)
        )
        click.echo(
            click.style("Outputs:", **name_style)
            + "\n"
            + json.dumps(output_json, indent=2, sort_keys=True)
        )
