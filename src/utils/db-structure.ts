function view(entity: string, props: string[]): string {
  // concat entity and props
  return `View:${entity}:${props.join(",")}`;
}

export {view};
